''' TCP server: receives instrument data files from the tcp cient running
   on Windows, and write the file contents to one of the /opt/gls/clarity/data
   directores '''

import os
import argparse
import socket
import threading

import data_path
from data_file_map import DATA_FILE_MAP
from gls_func_utils import get_logger, ProcessingError

class FileWriter(threading.Thread):
    ''' write file to disk and inform Windows client of progress '''
    def __init__(self, client, logger):
        #threading.Thread.__init__(self)
        super(FileWriter, self).__init__()
        self.logger = logger
        self.client = client

    def run(self):
        ''' process client request: write file contensts to directory '''
        file_descriptor = None
        bdone = False
        file_path = data_path.ROOT_DATA_PATH
        while True:
            try:
                data = self.client.recv(2048).decode('utf-8', errors='ignore')
                if data:
                    if "FILE_NAME" in data:
                        filename = data[9:]
                        lfilename = filename.lower()
                        for file_type, directory in DATA_FILE_MAP.items():
                            lfile_type = file_type.lower()
                            if lfile_type in lfilename:
                                for group_id in data_path.GROUP_IDS:
                                    if group_id.lower() in lfilename:
                                        file_path += group_id
                                        file_path += directory
                                        bdone = True
                                        break
                                if bdone:
                                    break

                        if not bdone:
                            err_msg = "ERROR"
                            self.client.send(err_msg.encode('utf-8'))
                            raise ProcessingError("Unrecognised file format name")

                        file_path += filename

                        file_descriptor = open(file_path, "w+")
                        self.client.send("FILENAME_RECEIVED".encode('utf-8'))
                    elif data == "END_OF_TRANSMISSION":
                        self.client.send("FILE_CONTENTS_RECEIVED".encode('utf-8'))
                        if file_descriptor:
                            file_descriptor.close()
                        break  # file trsnsfer complete
                    else:
                        if file_descriptor:
                            file_descriptor.write(data)
            except (OSError, IOError) as err:
                self.logger.error(str(err)) #ignore error, give client a chance to reconnect


class InstrumentFileServer:
    ''' service Windows client request '''
    def __init__(self, logger, port):
        self.logger = logger
        host = ''
        try:
            self.socket = socket.socket()
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((host, port))
            self.new_con = None
        except (OSError) as err:
            msg = "Error starting binding to the socket - " + str(err)
            self.logger.error(msg) #ignore error, give client a chance to reconnect

    def service_client_request(self):
        ''' call the thread run routine to process client request '''
        try:
            self.new_con.start()
        except (OSError, IOError, ProcessingError) as err:
            self.logger.error(str(err)) #ignore error, give client a chance to reconnect

    def run(self):
        ''' wait for client conncections '''
        while True:
            try:
                print("Listening...")
                self.socket.listen()
                client, addr = self.socket.accept()
                self.new_con = FileWriter(client, self.logger)
                self.service_client_request()
            except (OSError, IOError, ProcessingError) as err:
                self.logger.error(str(err)) #ignore error, give client a chance to reconnect


def main():
    ''' program main routine: extract program tokens from the cmd line arguments ''' 
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--portNumber", help="TCP port number")
    args = parser.parse_args()

    prog_name = os.path.basename(__file__)
    logger = get_logger(prog_name)

    ins_file_srv = InstrumentFileServer(logger, int(args.portNumber))
    ins_file_srv.run()


if __name__ == '__main__':
    main()
