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
                            set_exit_status,
                            ProcessingError)


class SampleDataReader:
    ''' read concentration data from the csv data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_concs_map = {}
        self.exist_picogreen = None

        group_id = group_id.strip()
        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.picogreen_filename = ("/opt/gls/clarity/data/{}/Concentrations/{}"
                                   .format(group_id, "PicogreenConcentrations.dat"))
        self.qubit_filename = ("/opt/gls/clarity/data/{}/Concentrations/{}"
                               .format(group_id, "QubitConcentrations.dat"))

    def extract_data(self):
        ''' read concentration data from the csv data file '''
        self.logger.info("Reading concentrations from the data file ...")
        filename = None
        if os.path.isfile(self.picogreen_filename):
            filename = self.picogreen_filename
            self.exist_picogreen = True
        else:
            filename = self.qubit_filename

        conc_data = []
        try:
            with open(filename, 'r') as file_descriptor:
                conc_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading concentration data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading concentration data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        for data in conc_data:
            sample_name = data[0]
            if sample_name not in self.sample_concs_map:
                self.sample_concs_map[sample_name] = []
            for field in data[1:0]:
                self.sample_concs_map[sample_name].append(field)

        for name, conc in self.sample_concs_map.items():
            self.logger.debug("sample_name: {}, data: {}".format(name, conc))

    def is_picogreen_data(self):
        ''' using picrogreen data or qubit data '''
        return self.exist_picogreen

    def empty_conc_files(self):
        ''' empty conc files of its data '''
        open(self.picogreen_filename, "w").close()
        open(self.qubit_filename, "w").close()

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
                self.gau_interface.setUDF(analyte_dom, "Concentration",
                                          self.sde_mgr.sample_concs_map[sample_name][0],
                                          "Numeric")
                self.logger.debug("Updating {} concentrations for sample...".format(sample_name))
                try:

                    self.gau_interface.setUDF(analyte_dom, "Concentration",
                                              self.sde_mgr.sample_concs_map[sample_name][0],
                                              "Numeric")

                    if self.sde_mgr.is_picogreen_data():
                        self.gau_interface.setUDF(analyte_dom, "Volume",
                                                  self.sde_mgr.sample_concs_map[sample_name][1],
                                                  "Numeric")

                    self.logger.debug(analyte_dom.toprettyxml())

                    rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                    analyte_dom = parseString(rsp_xml)
                    self.logger.debug(analyte_dom.toprettyxml())
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

        self.sde_mgr.empty_conc_files()

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
