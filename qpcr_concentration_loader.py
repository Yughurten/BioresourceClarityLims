''' this script reads concentrations stored in a csv file generated from
    a PCR instrument, updates the sample Average (nl) '''

import os
import csv
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_group_id,
                            get_sample_name,
                            get_analyte_name,
                            get_sample_concentration,
                            get_data_filename,
                            set_exit_status,
                            archive_file,
                            SampleValueDataReader,
                            ProcessingError)

class SampleDataReader:
    ''' read concentration data from the csv data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_concs_map = {}

        process_type = "qPCRC"
        group_id = group_id.strip()

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.filename = get_data_filename(process_type, group_id)

    def get_field_indices(self, conc_data):
        ''' get the indices of the file header fields '''
        sample_name_idx = -1
        ct_idx = -1
        qty_mean_idx = -1
        header_row_idx = -1
        for data_idx, row in enumerate(conc_data):
            for field_idx, field in enumerate(row):
                lfield = field.strip().lower()
                if lfield == "sample name":
                    sample_name_idx = field_idx
                if lfield == "ct":
                    ct_idx = int(field_idx)
                if lfield == "qty mean":
                    qty_mean_idx = int(field_idx)
            if sample_name_idx != -1 and ct_idx != -1 and qty_mean_idx != -1:
                header_row_idx = data_idx
                break

        if sample_name_idx == -1:
            err_msg = "Expected 'Sample Name' field missing in the qPCR data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if ct_idx == -1:
            err_msg = "Expected 'ct' field missing in the qPCR data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if qty_mean_idx == -1:
            err_msg = "Expected 'Qty Mean' field missing in the qPCR data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (header_row_idx, sample_name_idx, ct_idx, qty_mean_idx)

    def extract_data(self):
        ''' read concentration data from the csv data file '''
        conc_data = []
        self.logger.info("Reading concentrations from the data file ...")

        try:
            with open(self.filename, 'r') as file_descriptor:
                conc_data = list(csv.reader(file_descriptor))
        except IOError as err:
            archive_file(self.filename)
            err_msg = "Error reading concentration data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading concentration data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        (header_row_idx, sample_name_idx, ct_idx, qty_mean_idx) = self.get_field_indices(conc_data)

        for idx, data in enumerate(conc_data):
            if idx <= header_row_idx:
                continue
            sample_name = data[int(sample_name_idx)]
            if sample_name not in self.sample_concs_map:
                self.sample_concs_map[sample_name] = []
            if "std" in sample_name.lower():
                self.sample_concs_map[sample_name].append(data[ct_idx])
            else:
                self.sample_concs_map[sample_name].append(data[qty_mean_idx])

        for name, concs in self.sample_concs_map.items():
            for conc in concs:
                self.logger.debug("sample_name: {}, conc: {}".format(name, conc))

        archive_file(self.filename)


class QPCRConcentrationUpdaterMgr:
    ''' update the analyte Avearge (nl) using REST API '''
    def __init__(self, logger, gau_interface, step_uri, use_mode, group_id):
        self.logger = logger
        self.use_mode = use_mode
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []

        self.sde_mgr = SampleDataReader(logger, group_id)
        self.sde_mgr.extract_data()
        self.as_reader = SampleValueDataReader(logger, group_id, "AgilentSizes.dat")
        self.as_reader.extract_data()

        self.library_filename = ("/opt/gls/clarity/data/{}/Concentrations/{}"
                                 .format(group_id, "Library.dat"))

        self.library_file_descriptor = None
        try:
            self.library_file_descriptor = open(self.library_filename, "w")
        except IOError as err:
            err_msg = "Error opening molarity data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        header = "{},{}\n".format("Sample Name", "Value")
        self.library_file_descriptor.write(header)


    def build_analyte_uri_list(self):
        ''' build a list of analyte URIs '''
        self.logger.info("Building the artifacts list ...")
        process_xml = self.gau_interface.getResourceByURI(self.process_uri)
        process_dom = parseString(process_xml)
        self.logger.debug(process_dom.toprettyxml())

        ioutput_elements = process_dom.getElementsByTagName("input-output-map")
        for ioutput_elem in ioutput_elements:
            output_elem = ioutput_elem.getElementsByTagName("output")[0]
            if output_elem:
                output_type = output_elem.getAttribute("output-type")
                if (self.use_mode == "Avrg" and output_type == "ResultFile" or
                        self.use_mode == "Calc" and output_type == "Analyte"):
                #if output_type == "ResultFile":
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def update_qpcr_udfs(self):
        ''' update the analyte concentrations, average concentration and qc flag '''
        for analyte_uri in self.analyte_uri_list:
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)
            self.logger.debug("=================== ANALYTE DOM ==================================")
            self.logger.debug(analyte_dom.toprettyxml())


            #rsp_xml = None

            try:
                sample_name = get_sample_name(self.gau_interface, analyte_dom)
                analyte_name = get_analyte_name(analyte_dom)

                self.logger.debug("Updating {} concentrations ...".format(sample_name))

                conc = None
                if self.use_mode == "Avrg":
                    if sample_name not in self.sde_mgr.sample_concs_map:
                        sample_name = analyte_name
                    conc = get_sample_concentration(sample_name, self.sde_mgr.sample_concs_map)
                else:
                    conc = get_sample_concentration(analyte_name, self.sde_mgr.sample_concs_map)
                    sample_name = analyte_name

                conc = float(conc)
                self.gau_interface.setUDF(analyte_dom, "Average (nM)",
                                          conc,
                                          "Numeric")

                if self.use_mode == "Calc":
                    agilent_size = self.as_reader.get_agilent_size_value(analyte_name)
                    if not agilent_size:
                        err_msg = "Cannot find Agilent Size value for " + analyte_name
                        self.logger.debug(err_msg)
                        raise ProcessingError(err_msg)
                    self.logger.debug("agilent_size: {}".format(agilent_size))
                    agilent_size = float(agilent_size)
                    self.gau_interface.setUDF(analyte_dom, "Agilent Size",
                                              agilent_size,
                                              "Numeric")

                    library = (452 / agilent_size) * conc
                    self.logger.debug("library: {}".format(library))
                    self.gau_interface.setUDF(analyte_dom, "Library (nM)",
                                              library,
                                              "Numeric")

                    library_data = "{},{}\n".format(analyte_name, library)
                    self.logger.debug(library_data)
                    self.library_file_descriptor.write(library_data)

                self.gau_interface.updateObject(analyte_dom, analyte_uri)
            except Exception as err:
                err_msg = ("Error updating sample '{}' concentration: - {} "
                           .format(analyte_name, str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)


def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation and start processing '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-m", "--useMode", help="the mode of function (Conc or Calc)")
    parser.add_argument("-g", "--groupId", help="the work group that run this script")
    args = parser.parse_args()

    tokens = args.stepURI.split("/")
    hostname = "/".join(tokens[2:3])

    prog_name = os.path.basename(__file__)
    logger = get_logger(prog_name)

    log_cmdline_args(logger, prog_name, hostname, args.username, args.password, args.stepURI)

    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    program_status = None
    try:
        exit_msg = None
        exit_status = None
        qcu_mgr = QPCRConcentrationUpdaterMgr(logger, gau_interface, args.stepURI,
                                              args.useMode, args.groupId)
        qcu_mgr.build_analyte_uri_list()
        qcu_mgr.update_qpcr_udfs()
    except ProcessingError as err:
        if args.useMode == "Conc":
            exit_msg = "qPCR concentration load failed - {}".format(str(err))
        else:
            exit_msg = "qPCR calculation failed - {}".format(str(err))
            exit_status = "ERROR"

        program_status = 1
        logger.error(exit_msg)
    except Exception as err:
        if args.useMode == "Conc":
            exit_msg = "qPCR concentration load failed - {}".format(str(err))
        else:
            exit_msg = "qPCR calculation failed - {}".format(str(err))
            exit_status = "ERROR"
        program_status = 1
        logger.error(exit_msg)
    else:
        if args.useMode == "Conc":
            exit_msg = "qPCR concentration load completed successfully"
        else:
            exit_msg = "qPCR calculation completed successfully"
            exit_status = "OK"
        program_status = 0
        logger.info(exit_msg)
    finally:
        if args.useMode == "Calc":
            set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        else:
            print(exit_msg)

    return program_status


if __name__ == "__main__":
    main()
