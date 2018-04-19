''' set the cummulative QC srtatus of the CATGO DNA quantification workflow '''

import os
import csv
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_group_id,
                            get_analyte_name,
                            QCFlagsWriter,
                            ProcessingError
                           )

class QCFlagAggregator:
    ''' aggregate the QC status for the CATGO DNA x quantification steps '''
    def __init__(self, logger, gau_interface, step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []
        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.qc_flag_reader = QCFlagsWriter(logger, gau_interface, group_id)

    def cache_artifact(self, lims_id):
        ''' save artifact into a list '''
        if lims_id not in self.analyte_uri_list:
            self.analyte_uri_list.append(lims_id)

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
                output_elem_type = output_elem.getAttribute("output-type")
                if output_elem_type == "Analyte":
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def get_sample_qc_flags(self, sample_name):
        ''' get the qc_flag value from the analyte/conc map '''
        for sample, flags in self.qc_flag_reader.sample_qc_flag_map.items():
            if (sample_name in sample) or (sample in sample_name):
                return flags

        return None

    def update_qubit_udfs(self):
        ''' update the analyte ratios, average ratio and qc flag '''
    def update_qc_flag_values(self):
        ''' add qc flag values to the sample '''
        self.qc_flag_reader.read_qc_flags()
        self.build_analyte_uri_list()
        for analyte_uri in self.analyte_uri_list:
            self.logger.debug("Analyte URI: {}".format(analyte_uri))

            try:
                analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
                analyte_dom = parseString(analyte_xml)

                analyte_name = get_analyte_name(analyte_dom)

                qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                qc_flags = self.get_sample_qc_flags(analyte_name)
                self.logger.debug(str(qc_flags))
                if qc_flags:
                    fail_flag = None
                    for flag in qc_flags:
                        if flag == "FAILED":
                            fail_flag = True
                            break
                    if fail_flag:
                        qc_flag_elem.firstChild.replaceWholeText("FAILED")
                    else:
                        qc_flag_elem.firstChild.replaceWholeText("PASSED")
                    self.logger.debug(analyte_dom.toprettyxml())
                    rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                    #self.logger.debug(rsp_xml)
                    rsp_dom = parseString(rsp_xml)
                    self.logger.debug(rsp_dom.toprettyxml())
            except Exception as perr:
                err_msg = "Error updating QC flag value - " + str(perr)
                self.logger.debug(err_msg)
                raise ProcessingError(err_msg)

        self.qc_flag_reader.empty_qc_flags_file()

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automationi and do the auto placement '''

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-g", "--groupId", help="the work group that run this script")
    args = parser.parse_args()

    tokens = args.stepURI.split("/")
    hostname = "/".join(tokens[2:3])

    # setup logger object
    prog_name = os.path.basename(__file__)
    logger = get_logger(prog_name)

    log_cmdline_args(logger, prog_name, hostname, args.username, args.password, args.stepURI)

    # initiliase REST API interface
    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    exit_msg = None
    program_status = None
    try:
        qc_flag_aggregator = QCFlagAggregator(logger, gau_interface,
                                              args.stepURI, args.groupId)
        qc_flag_aggregator.update_qc_flag_values()
    except ProcessingError as perr:
        exit_msg = "Write of QC Flags failed - {}".format(str(perr))
        program_status = 1
    except Exception as err:
        exit_msg = "Write of QC Flags failed - {}".format(str(err))
        program_status = 1
    else:
        exit_msg = "Write of QC Flags competed successfully"
        program_status = 0
    finally:
        print(exit_msg)
        logger.debug(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
