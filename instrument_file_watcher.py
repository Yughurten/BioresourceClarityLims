''' This script watches for instrument files dropped by the lab scientists in various
    folders and sends them to the linux TCP server where the Clarity LIMS is runnng
    for further processing '''

import socket
import glob
import os
import shutil
import argparse
import logging
from time import sleep, strftime, gmtime

class ProcessingError(BaseException):
    ''' exception due to data processing '''
    def __init__(self, msg):
        super().__init__(msg)
        self._msg = msg
    def __str__(self):
        return self._msg

class InstrumentDataFileWasher:
    ''' this object make a connection with the Linux file server
        and sends instrument data file to it '''
    def __init__(self, ip_address, port_number):
        self.ip_address = ip_address
        self.port_number = port_number
        print(self.ip_address, self.port_number)
        log_format = "%(levelname)s %(asctime)s %(message)s"
        logging.basicConfig(filename="C:\\Clarity LIMS\\log\\InstrumentDataFileWasher.log",
                            level=logging.DEBUG,
                            format=log_format)
        self.logger = logging.getLogger()
        self.socket = None
        self.data = None

    def init_connection(self):
        ''' initialise the connection with server '''
        print(self.ip_address, self.port_number)
        try:
            self.socket = socket.socket()
            self.socket.settimeout(2)
            if self.socket:
                self.socket.connect((self.ip_address, self.port_number))
        except OSError as err:
            err_msg = "Error connecting to server - {}" .format(str(err))
            self.logger.error(err_msg)

    def close_connection(self):
        ''' close the connection with server '''
        if self.socket:
            self.socket.close()
            self.socket = None

    @staticmethod
    def get_date_and_timestamp():
        ''' get the time in the form 19_Feb_2018__13:58:15 '''
        dt_stamp = strftime("%d_%b_%Y__%H_%M_%S", gmtime())
        return dt_stamp

    def reconnect(self):
        ''' re-establish the connection with server '''
        self.close_connection()
        self.init_connection()

    def send_file_name(self, file_name):
        ''' send file name to server '''
        data = "FILE_NAME"
        data += file_name

        try:
            self.socket.send(data.encode('utf-8'))
            data = self.socket.recv(4096).decode('utf-8')
            if "FILENAME_RECEIVED" in data:
                msg = ("Send file name {} to Clarity file server successful"
                       .format(file_name))
                self.logger.info(msg)

            if "ERROR" in data:
                msg = "Error in file name: " + file_name
                self.logger.error(msg)

        except OSError as err:
            err_msg = ("Error sending file name {} to Clarity file server - {}"
                       .format(file_name, str(err)))
            self.logger.error(err_msg)

    def send_file_contents(self, local_source_path, contents):
        ''' send file contents ito server '''
        data_end = "END_OF_TRANSMISSION"
        try:
            self.socket.send(contents.encode('utf-8'))
            sleep(1)
            self.socket.send(data_end.encode('utf-8'))

            data = self.socket.recv(2048).decode('utf-8')
            if "FILE_CONTENTS_RECEIVED" not in data:
                msg = "Send file contents did not receive acknowledgment from Clarity File server"
                raise ProcessingError(msg)
            msg = ("Send file contents of ({}) to Clarity file server successful"
                   .format(local_source_path))
            self.logger.info(msg)
        except OSError as err:
            err_msg = ("Error sending file contents of ({}) to Clarity file server - {}"
                       .format(local_source_path, str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def send_file(self, file_name, local_source_path):
        ''' send file name and its contents to server '''
        self.send_file_name(file_name)
        file_descriptor = None
        try:
            file_descriptor = open(local_source_path, "r")
        except (OSError, IOError) as err:
            err_msg = ("Error opening file {} - {}" .format(local_source_path, str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        contents = None
        if file_descriptor:
            try:
                contents = file_descriptor.read()
            except OSError as err:
                err_msg = ("Error reading file: {} - {}" .format(local_source_path, str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
        if contents:
            file_descriptor.close()
            sleep(0.1)
            self.send_file_contents(local_source_path, contents)

    def archive_file(self, local_source_path, archive_path, file_name):
        ''' copy the sent file to the archive folder for safe keeping '''
        try:
            base_filename, file_extension = os.path.splitext(file_name)
            dt_stamp = self.get_date_and_timestamp()
            archive_dest_path = ("{}{}_{}{}"
                                 .format(archive_path, base_filename, dt_stamp, file_extension))
            path = shutil.copy(local_source_path, archive_dest_path)
            if path:
                os.remove(local_source_path)
        except shutil.Error as err:
            msg = ("Error copy file to archive {} - {}".format(file_name, str(err)))
            self.logger.error(msg)

    def run(self):
        ''' watch for file dropper by lab scientists in several folders
            make a conncetion with the server, send and archive file '''
        folder_list = ["C:\\Clarity LIMS\\Data\\Glomax\\",
                       "C:\\Clarity LIMS\\Data\\Qubit\\",
                       "C:\\Clarity LIMS\\Data\\qPCR\\",
                       "C:\\Clarity LIMS\\Data\\Picogreen\\",
                       "C:\\Clarity LIMS\\Data\\Trinean\\",
                       "C:\\Clarity LIMS\\Data\\5nMPlatePreparation\\",
                       "C:\\Clarity LIMS\\Data\\Adaptor\\",
                       "C:\\Clarity LIMS\\Data\\BioAnalyser\\",
                       "C:\\Clarity LIMS\\Data\\Caliper\\",
                       "C:\\Clarity LIMS\\Data\\Default\\",
                       "C:\\Clarity LIMS\\Data\\EndRepair\\",
                       "C:\\Clarity LIMS\\Data\\FinalLibraryMolarity\\",
                       "C:\\Clarity LIMS\\Data\\FinalPool\\",
                       "C:\\Clarity LIMS\\Data\\PCR\\",
                       "C:\\Clarity LIMS\\Data\\PoolingNormalisation\\",
                       "C:\\Clarity LIMS\\Data\\PoolSamples\\",
                       "C:\\Clarity LIMS\\Data\\PostHybWashes\\"
                      ]

        while True:
            for data_folder in folder_list:
                glob_folder = data_folder + "*"
                for local_source_path in glob.glob(glob_folder):
                    if os.path.isfile(local_source_path) and 'csv' in local_source_path:
                        tokens = local_source_path.split("\\")
                        file_name = tokens[4]
                        print(file_name)
                        try:
                            self.reconnect()
                            self.send_file(file_name, local_source_path)
                            archive_foldder = data_folder + "Archives\\"
                            self.archive_file(local_source_path, archive_foldder, file_name)
                        except ProcessingError as err:
                            msg = "{} - {}".format(local_source_path, str(err))
                            self.logger.error(msg)
                            sleep(10)  # network problem, retry in 10 seconds
                        except Exception as err:
                            msg = "{} - {}".format(local_source_path, str(err))
                            self.logger.error(msg)
                            sleep(10)  # network problem, retry in 10 seconds

                sleep(.5)


def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments and start the file watcher '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--ipAddress", help="IP Address of file server")
    parser.add_argument("-p", "--portNumber", help="Port number of file server")
    args = parser.parse_args()

    ins_file_washer = InstrumentDataFileWasher(args.ipAddress, int(args.portNumber))
    ins_file_washer.run()


if __name__ == '__main__':
    main()
