''' tcp server: receives barcode data fileis from the tcp cient running
    on Linux, and write the file contents to to the C:/Clarity LIMS/Data/Bracodes
    directory interface module to print to a Zebra printer '''

import sys
import socket
import threading
import logging
from time import strftime, gmtime
import win32print

class ProcessingError(BaseException):
    ''' exception due to data processing '''
    def __init__(self, msg):
        super().__init__(msg)
        self._msg = msg
    def __str__(self):
        return self._msg


class ZebraBarcodeWriter(object):
    ''' a class to communicate with (Zebra) label printers using EPL2'''
    def __init__(self, logger, queue=None):
        ''' queue - name of the printer queue (optional)'''
        self.queue = queue
        self.logger = logger

    def _output(self, commands):
        if self.queue == 'zebra_python_unittest':
            print(commands)
            return
        hprinter = win32print.OpenPrinter(self.queue)
        try:
            #hJob = win32print.StartDocPrinter(hprinter, 1, ('Label', None, 'RAW'))
            win32print.StartDocPrinter(hprinter, 1, ('Label', None, 'RAW'))
            try:
                win32print.StartPagePrinter(hprinter)
                win32print.WritePrinter(hprinter, commands)
                win32print.EndPagePrinter(hprinter)
            finally:
                win32print.EndDocPrinter(hprinter)
        finally:
            win32print.ClosePrinter(hprinter)

    def output(self, commands):
        ''' Output EPL2 commands to the label printer
        commands - EPL2 commands to send to the printer
        '''
        assert self.queue is not None
        if sys.version_info[0] == 3:
            if isinstance(commands, bytes):
                commands = str(commands).encode()
        else:
            commands = str(commands).encode()
            self._output(commands)

    @staticmethod
    def _get_queues():
        ''' get the list of printers connected to this Windows machine '''
        printers = []
        for (_, _, name, _) in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL):
            printers.append(name)
        return printers

    def get_queues(self):
        ''' Returns a list of printer queues on local machine'''
        return self._get_queues()

    def set_queue(self, queue):
        ''' Set the printer queue'''
        self.queue = queue

    def setup(self, direct_thermal=None, label_height=None, label_width=None):
        ''' Set up the label printer. Parameters are not set if they are None.

        direct_thermal - True if using direct thermal labels
        label_height   - tuple (label height, label gap) in dots
        label_width    - in dots
        '''
        commands = '\n'
        if direct_thermal:
            commands += 'OD\n'
        if label_height:
            commands += "Q{},{}\n".format(label_height[0], label_height[1])
        if label_width:
            commands += 'q{}\n'.format(label_width)
        self.output(commands)


class BarcodeWriter(threading.Thread):
    ''' write file to disk and inform Windows client of progress '''
    def __init__(self, client, logger):
        print(client, logger)
        super(BarcodeWriter, self).__init__()
        self.logger = logger
        self.client = client
        print(client, logger)
        self.zbw = ZebraBarcodeWriter(logger)
        print("Printer queues found: {}".format(self.zbw.get_queues()))
        self.zbw.set_queue('zebra_python_unittest')
        self.zbw.setup(direct_thermal=True, label_height=(406, 32),
                       label_width=609)    # 3" x 2" direct thermal label

    @staticmethod
    def get_date_and_timestamp():
        ''' get the time in the form 19_Feb_2018__13:58:15 '''
        dt_stamp = strftime("%d_%b_%Y__%H_%M_%S", gmtime())
        return dt_stamp

    def run(self):
        ''' process client request: write file contensts to directory '''
        print("run called....")
        file_descriptor = None
        barcode_data = None
        while True:
            try:
                data = self.client.recv(2048).decode('utf-8', errors='ignore')
                print("=======>    data: ", data)
                exit()
                if data:
                    if "START_OF_TRANSMISSION" in data:
                        dt_stamp = self.get_date_and_timestamp()
                        file_path = ("C:/Clarity LIMS/data/Barcodes/Barcodes_{}"
                                     .format(dt_stamp))
                        print(file_path)
                        file_descriptor = open(file_path, "w+")
                    elif data == "END_OF_TRANSMISSION":
                        self.client.send("BARCODE_DATA_RECEIVED".encode('utf-8'))
                        if file_descriptor:
                            file_descriptor.close()
                        break  # file trsnsfer complete
                    else:
                        barcode_data += data
                        if file_descriptor:
                            file_descriptor.write(data)
            except (OSError, IOError) as err:
                self.logger.error(str(err)) #ignore error, give client a chance to reconnect
                print(str(err)) #ignore error, give client a chance to reconnect
            else:
                self.print_barcodes(barcode_data)

    def print_barcodes(self, barcode_data):
        ''' print each barcode spearately '''
        return
        barcodes = barcode_data.split("|")
        for barcode in barcodes:
            zpl = """
^XA
^FO150,40^BY3
^BCN,110,Y,N,N
"""
            zpl += "\n"
            zpl += "^FD"
            zpl += barcode
            zpl += "^FSi\n"
            zpl += "^XZ"

        self.zbw.output(zpl)


class BarcodeServerMgr:
    ''' service Windows client request '''
    def __init__(self):
        print(" __init__()")
        host = '0.0.0.0'
        port = 5020
        self.new_con = None
        try:
            self.socket = socket.socket()
            print(" __init__() 1")
            #self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            print(" __init__() 2")
            self.socket.bind((host, port))
            print(" __init__() 3")
            log_format = "%(levelname)s %(asctime)s %(message)s"
            logging.basicConfig(filename="C:\\Clarity LIMS\\log\\BarcodeDataFileWasher.log",
                                level=logging.DEBUG,
                                format=log_format)
            self.logger = logging.getLogger()
            self.data = None

        except (OSError) as err:
            err_msg = "Error starting binding to the socket - " + str(err)
            self.logger.error(err_msg) #ignore error, give client a chance to reconnect
            print(err_msg)

    def service_client_request(self):
        ''' call the thread run routine to process client request '''
        print("About to call start()")
        try:
            self.new_con.start()
            print("start() called")
        except (OSError, IOError, ProcessingError) as err:
            self.logger.error(str(err)) #ignore error, give client a chance to reconnect

    def run(self):
        ''' wait for client conncections '''
        print("run()")
        while True:
            try:
                print("run() 1")
                self.socket.listen()
                print("run() 2")
                client, addr = self.socket.accept()
                print(client, addr)
                print("run() 3")
                self.new_con = BarcodeWriter(client, self.logger)
                print("run() 4")
                self.service_client_request()
                print("run() 5")
            except (OSError, IOError, ProcessingError) as err:
                self.logger.error(str(err)) #ignore error, give client a chance to reconnect


def main():
    ''' main routine '''
    ins_file_srv = BarcodeServerMgr()
    ins_file_srv.run()


if __name__ == '__main__':
    main()
