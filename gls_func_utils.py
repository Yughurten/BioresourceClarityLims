'''utility functions used by the Clarity LIMS extension
   and customasation scripts '''

import csv
import os
import shutil
import logging
from time import strftime, gmtime
from xml.dom.minidom import parseString

import data_path
import data_file_map

class ProcessingError(BaseException):
    ''' exception due to data processing '''
    def __init__(self, msg):
        super().__init__(msg)
        self._msg = msg
    def __str__(self):
        return self._msg


def validate_layout_type(layout_type):
    ''' validate the layout type '''
    llayout_type = layout_type.lower()
    for ltype in data_file_map.DATA_FILE_MAP:
        if llayout_type == ltype.lower():
            return True

    return False


def validate_group_id(group_id):
    ''' validate the group id '''
    lgroup_id = group_id.lower()
    for grp_id in data_path.GROUP_IDS:
        if lgroup_id == grp_id.lower():
            return True

    return False


def make_search_directory(layout_type, group_id, filename="*"):
    ''' given a layout type and a group id make the file path '''
    file_path = None
    llayout_type = layout_type.lower()
    for ltype, directory in data_file_map.DATA_FILE_MAP.items():
        if llayout_type == ltype.lower():
            file_path = "{}{}{}{}".format(data_path.ROOT_DATA_PATH,
                                          group_id,
                                          directory,
                                          filename)
            break

    return file_path


def get_layout_filename(layout_type, group_id):
    ''' return the latest layout type data file name '''
    search_dir = make_search_directory(layout_type, group_id, "")
    files = [file for file in os.listdir(search_dir) if file.endswith(".csv")
             and layout_type in file
             and group_id in file
            ]
    if not files:
        err_msg = "Missing plate layout file for " + layout_type
        raise ProcessingError(err_msg)

    paths = [(search_dir + file) for file in files]
    return max(paths, key=os.path.getctime)


def get_data_filename(process_type, group_id):
    ''' get the latest instrument data file '''
    try:
        filename = get_layout_filename(process_type, group_id)
    except:
        err_msg = "Missing instrument data file for " + process_type
        raise ProcessingError(err_msg)

    return filename


def archive_file(filename):
    ''' timestamp and copy a file to the archive directory '''
    base_filename = os.path.basename(filename)
    base_filename, file_extension = os.path.splitext(base_filename)
    dirname = os.path.dirname(filename)
    dt_stamp = get_date_and_timestamp()
    archive_dest_path = ("{}/Archives/{}_{}{}"
                         .format(dirname, base_filename, dt_stamp, file_extension))
    shutil.copy(filename, archive_dest_path)
    os.remove(filename)


def get_sample_name(gau_interface, analyte_dom):
    ''' extract the sample name from the sample xml load '''
    sample_elem = analyte_dom.getElementsByTagName("sample")[0]
    if sample_elem:
        sample_uri = sample_elem.getAttribute("uri")
    else:
        raise ProcessingError("Cannot find sample uri from analyte dom")
    sample_xml = gau_interface.getResourceByURI(sample_uri)
    sample_dom = parseString(sample_xml)

    #self.logger.debug(sample_dom.toprettyxml())

    sample_name = None
    name_elem = sample_dom.getElementsByTagName("name")[0]
    if name_elem:
        sample_name = name_elem.firstChild.data
    else:
        raise ProcessingError("Cannot find name in name element")

    return sample_name


def get_analyte_name(analyte_dom):
    ''' extract the analyte name from the analyte xml load '''
    analyte_name = None
    name_elem = analyte_dom.getElementsByTagName("name")[0]
    if name_elem:
        analyte_name = name_elem.firstChild.data
    else:
        raise ProcessingError("Cannot find name in name element")

    return analyte_name


def get_sample_concentration(name, sample_map):
    ''' get the concentation value from the analyte/conc map '''
    if name in sample_map:
        conc = sample_map[name]
        return conc[0] if isinstance(conc, list) else conc

    for sample, conc in sample_map.items():
        if (name in sample) or (sample in name):
            return conc[0] if isinstance(conc, list) else conc

    err_msg = "Missing concentration data for sample: {} ".format(name)
    raise ProcessingError(err_msg)


def is_location_in_a_map(location, this_map):
    ''' cannot use key in a map as key is of the form A:10 '''
    for key in this_map:
        if key == location:
            return True

    return False


def validate_well_location(well_type, well_location):
    ''' validate the wll location:  C1 or M22 '''
    location_len = len(well_location)
    row = well_location[0]
    column = int(well_location[1:])

    len_check = location_len == 2 or location_len == 3

    if "96" in well_type:
        if (len_check and
                (row >= "A" and row <= "H") and
                (column >= 1 and column <= 12)
           ):
            return True

    if "384" in well_type:
        if (len_check and
                (row >= "A" and row <= "P") and
                (column >= 1 and  column <= 24)
           ):
            return True

    err_msg = "Invalid well location: "
    err_msg += well_location
    raise ProcessingError(err_msg)


def set_exit_status(gau_interface, step, status, msg):
    ''' set script exit status - update the status
       and add the message to the XML payload '''
    program_status_uri = step + "/programstatus"
    program_status_xml = gau_interface.getResourceByURI(program_status_uri)
    program_status_dom = parseString(program_status_xml)

    # change status
    status_elem = program_status_dom.getElementsByTagName("status")[0]
    if status_elem:
        status_elem.firstChild.replaceWholeText(status)

    # add the message
    msg_elem = program_status_dom.createElement('message')
    msg_txt = program_status_dom.createTextNode(msg)
    msg_elem.appendChild(msg_txt)
    program_status_dom.childNodes[0].appendChild(msg_elem)

    gau_interface.updateObject(program_status_dom, program_status_uri)


def get_error_message(rsp_xml):
    ''' get any error from exceptioni message '''
    msg = None
    resp_dom = parseString(rsp_xml)
    try:
        elem = resp_dom.getElementsByTagName("message")[0]
        if elem:
            msg = elem.firstChild.nodeValue
    except:
        pass   # no error message found

    return msg


def get_well_number(well_location):
    ''' given a well location extract and return the well number
       D:12   return 12 '''
    tokens = well_location.split(":")
    well_number = int(tokens[1])

    if well_number < 1 or well_number > 12:
        raise Exception("Incorrect well location")

    return well_number


def log_cmdline_args(logger, prog_name, hostname,
                     username, password, step_uri):
    ''' log the command line arguments passed to the script by Clarity '''
    msg = "{} started ...".format(prog_name)
    parameters = ("\nhostname: {}\nusername: {}\npassword: {}\nstep: {}\n"
                  .format(hostname, username, password, step_uri))
    logger.debug(msg)
    logger.debug(parameters)


def get_date_and_timestamp():
    ''' get the time in the form 19_Feb_2018__13:58:15 '''
    dt_stamp = strftime("%d_%m_%Y__%H_%M_%S", gmtime())
    return dt_stamp


def get_logger(prog_name):
    ''' create a logger handle to be used by the customisation scripts '''
    dt_stamp = get_date_and_timestamp()
    log_file_path = ("/opt/gls/clarity/log/{}_{}.log"
                     .format(prog_name.rstrip(".py"), dt_stamp))

    log_format = "%(levelname)s %(asctime)s %(message)s"
    logging.basicConfig(filename=log_file_path,
                        level=logging.DEBUG,
                        #level=logging.INFO,
                        format=log_format)

    logger = logging.getLogger()
    log_file_name = prog_name.strip(".py") + ".log"
    msg = "========  LogFileName: " + log_file_name
    logger.info(msg)

    return logger


class SampleValueDataReader:
    ''' read library data from the csv data file '''
    def __init__(self, logger, group_id, filename):
        self.logger = logger
        self.filename = None
        self.sample_value_map = {}

        group_id = group_id.strip()

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.filename = ("/opt/gls/clarity/data/{}/Concentrations/{}"
                         .format(group_id, filename))

    def get_field_indices(self, field_names):
        ''' get the indices of the file header fields '''
        self.logger.debug(field_names)
        sample_name_idx = None
        value_idx = None
        for idx, field in enumerate(field_names):
            self.logger.debug(field)
            lfield = field.strip().lower()
            if lfield == "sample name":
                sample_name_idx = idx
            if lfield == "value":
                value_idx = idx
            elif sample_name_idx and value_idx:
                break

        if (sample_name_idx != 0) and (not sample_name_idx):
            err_msg = "Expected 'Sample Name' field missing in the data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if (value_idx != 0) and (not value_idx):
            err_msg = "Expected 'Agilent Size' field missing in the Agilent Size data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (sample_name_idx, value_idx)

    def extract_data(self):
        ''' read sample/value data from the csv data file '''
        sample_value_data = []
        self.logger.info("Reading values from the data file ...")

        try:
            with open(self.filename, 'r') as file_descriptor:
                sample_value_data = list(csv.reader(file_descriptor))
                self.logger.error(sample_value_data)
        except IOError as err:
            err_msg = "Error reading concentration data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug(len(sample_value_data))
        field_names = sample_value_data[0]
        self.logger.debug(field_names)

        (sample_name_idx, value_idx) = self.get_field_indices(field_names)

        for data in sample_value_data[1:]:
            sample_name = data[sample_name_idx]
            if not sample_name:
                err_msg = "Invalid Sample Name: {}".format(sample_name)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            value = float(data[value_idx])
            if value <= 0:
                err_msg = ("Invalid value: {} for sample name: {}"
                           .format(value, sample_name))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            if sample_name not in self.sample_value_map:
                self.sample_value_map[sample_name] = value

        for name, value in self.sample_value_map.items():
            self.logger.debug("sample_name: {0:>8}, value: {1:>5.1f}"
                              .format(name, value))

    def get_value(self, analyte_name):
        ''' get the value from the analyte/value map '''
        if analyte_name in self.sample_value_map:
            return self.sample_value_map[analyte_name]

        for sample, value in self.sample_value_map.items():
            if analyte_name in sample or sample in analyte_name:
                return value

        err_msg = "Missing data value for sample: {} ".format(analyte_name)
        self.logger.error(err_msg)
        raise ProcessingError(err_msg)

    def get_agilent_size_value(self, analyte_name):
        ''' get agilent size for this analyte '''
        return self.get_value(analyte_name)

    def get_library_value(self, analyte_name):
        ''' get agilent size for this analyte '''
        return self.get_value(analyte_name)


class QCFlagsReaderWriter:
    ''' set the QC status for the CATGO  quantification '''
    def __init__(self, logger, gau_interface, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.file_descriptor = None
        self.sample_qc_flag_map = {}

        self.filename = ("/opt/gls/clarity/data/{}/Concentrations/QCFlags.dat"
                         .format(group_id))

    def read_qc_flags(self):
        ''' read qc flags from the csv data file '''
        self.logger.info("Reading QC flags from the data file ...")

        try:
            with open(self.filename, 'r') as file_descriptor:
                qc_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading concentration data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading QC flag data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        file_descriptor.close()

        for data in qc_data:
            sampe_name = data[0]
            if sampe_name not in self.sample_qc_flag_map:
                self.sample_qc_flag_map[sampe_name] = []
            size = len(data[1:])
            self.logger.debug("size: {}".format(size))
            for flags in data[1:]:
                self.sample_qc_flag_map[sampe_name].append(flags)
                self.logger.debug("sample_name: {}".format(sampe_name))

        for name, qc_flags in self.sample_qc_flag_map.items():
            self.logger.debug("sampe_name: {0:>8}".format(name))
            flags = ""
            size = len(qc_flags)
            self.logger.debug("flag number: {}".format(size))
            for idx, flag in enumerate(qc_flags):
                flags += flag[idx - 1]
                if idx < size - 1:
                    flags += ","
            if flags:
                self.logger.debug("{}: {}".format(name, flags))

    def add_qc_flag(self, sample_name, qc_flag):
        ''' append qc flag to the csv data file '''
        self.logger.info("Adding QC flag to the map ...")
        if sample_name in self.sample_qc_flag_map:
            self.sample_qc_flag_map[sample_name].append(qc_flag)
            return

        for sample in self.sample_qc_flag_map:
            if sample_name in sample or sample in sample_name:
                self.sample_qc_flag_map[sample_name].append(qc_flag)
                return

        self.sample_qc_flag_map[sample_name] = []
        self.sample_qc_flag_map[sample_name].append(qc_flag)

    def save_qc_flags(self):
        ''' write qc flag data to the csv data file '''
        try:
            file_descriptor = open(self.filename, 'w')
        except IOError as err:
            err_msg = "Error writing QC flag to data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error opening QC flag data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        try:
            for sample, qc_flags in self.sample_qc_flag_map.items():
                record = sample
                record += ","
                size = len(qc_flags)
                for idx, flag in enumerate(qc_flags):
                    record += flag
                    if idx < size - 1:
                        record += ","
                record += "\n"
                file_descriptor.write(record)
        except Exception as err:
            err_msg = ("Error writing Qc flag data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        file_descriptor.close()

    def empty_qc_flags_file(self):
        ''' empty qc flags file of its data '''
        open(self.filename, "w").close()


class ConcentrationWriter:
    ''' set the Concentration data to a file '''
    def __init__(self, logger, gau_interface, filename):
        self.logger = logger
        self.gau_interface = gau_interface
        self.file_descriptor = None
        self.sample_conc_map = {}

        self.filename = filename

    def add_conc_value(self, sample_name, conc_value):
        ''' append qc flag to the csv data file '''
        self.logger.info("Adding Concentration to the map ...")
        if sample_name in self.sample_conc_map:
            self.sample_conc_map[sample_name].append(conc_value)
            return

        for sample in self.sample_conc_map:
            if sample_name in sample or sample in sample_name:
                self.sample_conc_map[sample_name].append(conc_value)
                return

        self.sample_conc_map[sample_name] = []
        self.sample_conc_map[sample_name].append(conc_value)

    def save_conc_values(self):
        ''' write qc flag data to the csv data file '''
        try:
            file_descriptor = open(self.filename, 'w')
        except IOError as err:
            err_msg = "Error concentration data to file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error opening QC flag data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        try:
            for sample, concs in self.sample_conc_map.items():
                record = sample
                record += ","
                size = len(concs)
                for idx, conc in enumerate(concs):
                    record += str(conc)
                    if idx < size - 1:
                        record += ","
                record += "\n"
                file_descriptor.write(record)
        except Exception as err:
            err_msg = ("Error writing Qc flag data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        file_descriptor.close()

    def empty_concentration_file(self):
        ''' empty concentration file of its data '''
        open(self.filename, "w").close()
