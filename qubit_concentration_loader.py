''' this script reads concentrations stored in a csv file generated from
    a Qubit instrument, updates the sample first and second concentrations,
    calculates the arithmetic average, and updates the sample QC flag '''

import os
import csv
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_group_id,
                            get_sample_name,
                            get_data_filename,
                            set_exit_status,
                            archive_file,
                            ProcessingError)


class SampleDataReader:
    ''' read concentration data from the csv data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_concs_map = {}

        process_type = "QBTC"
        group_id = group_id.strip()

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.filename = get_data_filename(process_type, group_id)

    def get_field_indices(self, field_names):
        ''' get the indices of the file header fields '''
        test_name_idx = None
        sample_concentration_idx = None
        for idx, field in enumerate(field_names):
            lfield = field.strip().lower()
            if lfield == "test name":
                test_name_idx = idx
            if lfield == "original sample conc.":
                sample_concentration_idx = idx
            elif test_name_idx and sample_concentration_idx:
                break

        if test_name_idx != 0 and test_name_idx is None:
            err_msg = "Expected 'Test Name' field missing in the Qubit data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        print(test_name_idx, sample_concentration_idx)
        if sample_concentration_idx != 0 and sample_concentration_idx is None:
            err_msg = "Expected 'Original sample conc.' field missing in the Qubit data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (test_name_idx, sample_concentration_idx)

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

        field_names = list(conc_data[0])
        self.logger.debug(field_names)

        (test_name_idx, sample_concentration_idx) = self.get_field_indices(field_names)

        size = len(conc_data[1:])
        if size == 0 or size % 2 != 0:
            err_msg = "Invalid number of concentation rows: {}".format(size)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        for data in conc_data[1:]:
            test_name = (data[test_name_idx]).strip()
            if test_name not in self.sample_concs_map:
                self.sample_concs_map[test_name] = []
            if len(test_name) <= 0:
                archive_file(self.filename)
                err_msg = "Invalid Test Name: {}".format(test_name)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            conc = float(data[sample_concentration_idx])
            if conc <= 0:
                archive_file(self.filename)
                err_msg = ("Invalid concentration value: {} for Sample name: {}"
                           .format(conc, test_name))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            self.sample_concs_map[test_name].append(conc)

            if len(self.sample_concs_map[test_name]) == 2:
                conc1 = self.sample_concs_map[test_name][0]
                conc2 = self.sample_concs_map[test_name][1]
                average = 0.5 * (conc1 + conc2)
                self.sample_concs_map[test_name].append(average)

        for sample_name in self.sample_concs_map:
            concs = self.sample_concs_map[sample_name]
            if len(concs) != 3:
                archive_file(self.filename)
                err_msg = "Missing concentration(s) for sample: {}".format(sample_name)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

        for name, conc in self.sample_concs_map.items():
            self.logger.debug("test_name: {0:>8}, sc1: {1:>5.1f}, sc2: {2:>5.1f}, Avg: {3:>5.1f}"
                              .format(name, conc[0], conc[1], conc[2]))

        archive_file(self.filename)

class QubitConcentrationUpdaterMgr:
    ''' update the analyte concentrations using REST API '''
    def __init__(self, logger, gau_interface, use_mode,
                 step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.use_mode = use_mode
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []

        self.sde_mgr = SampleDataReader(logger, group_id)
        self.sde_mgr.extract_data()

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
                all_concs = (self.use_mode == "All" and
                             output_elem.getAttribute("output-type") == "ResultFile")
                average = (self.use_mode == "Average" and
                           output_elem.getAttribute("output-type") == "Analyte")
                if  all_concs or average:
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)


    def update_qubit_udfs(self):
        ''' update the analyte concentrations, average concentration and qc flag '''
        for analyte_uri in self.analyte_uri_list:
            self.logger.debug("Analyte URI: {}".format(analyte_uri))
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)

            sample_name = get_sample_name(self.gau_interface, analyte_dom)
            self.logger.debug("sample_name: {}".format(sample_name))

            if sample_name in self.sde_mgr.sample_concs_map:
                self.logger.debug("Updating {} concentrations for sample...".format(sample_name))
                try:
                    if self.use_mode == "All":
                        qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                        if self.sde_mgr.sample_concs_map[sample_name][2] > 15.0:
                            qc_flag_elem.firstChild.replaceWholeText("PASSED")
                        else:
                            qc_flag_elem.firstChild.replaceWholeText("FAILED")

                        self.gau_interface.setUDF(analyte_dom, "Qubit 1st",
                                                  self.sde_mgr.sample_concs_map[sample_name][0],
                                                  "Numeric")
                        self.gau_interface.setUDF(analyte_dom, "Qubit 2nd",
                                                  self.sde_mgr.sample_concs_map[sample_name][1],
                                                  "Numeric")
                    self.gau_interface.setUDF(analyte_dom, "Qubit Average",
                                              self.sde_mgr.sample_concs_map[sample_name][2],
                                              "Numeric")

                    self.logger.debug(analyte_dom.toprettyxml())

                    rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                except Exception as err:
                    err_msg = "Error updating concencentations - " + str(err)
                    self.logger.error(err_msg)
                    raise ProcessingError(err_msg)

                rsp_dom = parseString(rsp_xml)
                self.logger.debug(rsp_dom.toprettyxml())
            else:
                err_msg = "Missing concentration data for sample: {} ".format(sample_name)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)


def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation and start the processing '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-m", "--useMode",
                        help="loading all concentrations or just the average concentration",
                        default="All")
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
        exit_status = None
        exit_msg = None
        qcu_mgr = QubitConcentrationUpdaterMgr(logger, gau_interface, args.useMode,
                                               args.stepURI, args.groupId)
        qcu_mgr.build_analyte_uri_list()
        qcu_mgr.update_qubit_udfs()
    except ProcessingError as err:
        exit_status = "ERROR"
        exit_msg = "Qubit concentration load failed - {}".format(str(err))
        program_status = 1
    except Exception as err:
        exit_status = "ERROR"
        exit_msg = "Qubit concentration load failed - {}".format(str(err))
        program_status = 1
    else:
        exit_status = "OK"
        exit_msg = "Qubit concentration load completed successfully"
        program_status = 0
    finally:
        if args.useMode == "Average":
            set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        else:
            print(exit_msg)
        logger.info(exit_msg)

    return program_status


if __name__ == "__main__":
    main()
