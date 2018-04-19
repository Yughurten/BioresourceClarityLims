''' this script reads ratios stored in a csv file generated from
    a Trinean instrument, updates the sample A260/A280 ratio '''

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
                            archive_file,
                            QCFlagsReaderWriter,
                            ProcessingError)

class SampleDataReader:
    ''' read A260/A280 data from the csv data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_ratios_map = {}

        process_type = "TRNNC"
        group_id = group_id.strip()

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.filename = get_data_filename(process_type, group_id)

    def get_field_indices(self, field_names):
        ''' get the indices of the file header fields '''
        sample_name_idx = None
        sample_ratio_idx = None
        for idx, field in enumerate(field_names):
            lfield = field.strip().lower()
            if lfield == "sample name":
                sample_name_idx = idx
            if lfield == "a260/a280":
                sample_ratio_idx = idx
            elif sample_name_idx and sample_ratio_idx:
                break

        if sample_name_idx != 0 and sample_name_idx is None:
            archive_file(self.filename)
            err_msg = "Expected 'Sample Name' field missing in the Trinean data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        print(sample_name_idx, sample_ratio_idx)
        if sample_ratio_idx != 0 and sample_ratio_idx is None:
            archive_file(self.filename)
            err_msg = "Expected 'A260/A280' field missing in the Trinean data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (sample_name_idx, sample_ratio_idx)

    def extract_data(self):
        ''' read ratio data from the csv data file '''
        ratio_data = []
        self.logger.info("Reading ratios from the data file ...")

        try:
            with open(self.filename, 'r') as file_descriptor:
                ratio_data = list(csv.reader(file_descriptor))
        except IOError as err:
            archive_file(self.filename)
            err_msg = "Error reading ratio data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading ratio data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        field_names = list(ratio_data[0])
        self.logger.debug(field_names)

        (sample_name_idx, sample_ratio_idx) = self.get_field_indices(field_names)

        for data in ratio_data[2:]:
            sample_name = data[sample_name_idx]
            if sample_name not in self.sample_ratios_map:
                self.sample_ratios_map[sample_name] = []
            if len(sample_name) <= 0:
                archive_file(self.filename)
                err_msg = "Invalid Sample Name: {}".format(sample_name)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            conc = float(data[sample_ratio_idx])
            if conc <= 0:
                archive_file(self.filename)
                err_msg = ("Invalid ratio value: {} for Sample name: {}"
                           .format(conc, sample_name))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            self.sample_ratios_map[sample_name] = conc


        for name, conc in self.sample_ratios_map.items():
            self.logger.debug("sample_name: {0:>8}, sc1: {1:>5.1f}".format(name, conc))

        archive_file(self.filename)

class TrineanConcentrationUpdaterMgr:
    ''' update the analyte ratios using REST API '''
    def __init__(self, logger, gau_interface, step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []

        self.sde_mgr = SampleDataReader(logger, group_id)
        self.sde_mgr.extract_data()
        self.qc_flag_reader = QCFlagsReaderWriter(logger, gau_interface, group_id)

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
                if output_elem.getAttribute("output-type") == "ResultFile":
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def get_sample_ratio(self, name):
        ''' get the ratio value from the analyte/ratio map '''
        if name in self.sde_mgr.sample_ratios_map:
            ratio = self.sde_mgr.sample_ratios_map[name]
            return ratio

        for sample, ratio in self.sde_mgr.sample_ratios_map.items():
            if (name in sample) or (sample in name):
                return ratio

        err_msg = "Missing ratio data for sample: {} ".format(name)
        raise ProcessingError(err_msg)

    def update_qubit_udfs(self):
        ''' update the analyte ratios, average ratio and qc flag '''
        self.qc_flag_reader.read_qc_flags()
        for analyte_uri in self.analyte_uri_list:
            self.logger.debug("Analyte URI: {}".format(analyte_uri))
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)

            sample_name = get_sample_name(self.gau_interface, analyte_dom)
            self.logger.debug("sample_name: {}".format(sample_name))

            self.logger.debug("Updating {} ratios for sample...".format(sample_name))
            try:
                ratio = self.get_sample_ratio(sample_name)
                qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                if ratio >= 1.75 and ratio <= 2.04:
                    qc_flag_elem.firstChild.replaceWholeText("PASSED")
                    self.qc_flag_reader.add_qc_flag(sample_name, "PASSED")
                else:
                    qc_flag_elem.firstChild.replaceWholeText("FAILED")
                    self.qc_flag_reader.add_qc_flag(sample_name, "FAILED")

                self.gau_interface.setUDF(analyte_dom, "A260/280 ratio",
                                          ratio,
                                          "Numeric")

                self.logger.debug(analyte_dom.toprettyxml())

                rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
            except Exception as err:
                err_msg = "Error updating A260/A280 ratio - " + str(err)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            rsp_dom = parseString(rsp_xml)
            self.logger.debug(rsp_dom.toprettyxml())

        self.qc_flag_reader.save_qc_flags()

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation and start the processing '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
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
        qcu_mgr = TrineanConcentrationUpdaterMgr(logger, gau_interface,
                                                 args.stepURI, args.groupId)
        qcu_mgr.build_analyte_uri_list()
        qcu_mgr.update_qubit_udfs()
    except ProcessingError as err:
        exit_msg = "Trinean ratio load failed - {}".format(str(err))
        program_status = 1
    except Exception as err:
        exit_msg = "Trinean ratio load failed - {}".format(str(err))
        program_status = 1
    else:
        exit_msg = "Trinean ratio load completed successfully"
        program_status = 0
    finally:
        print(exit_msg)
        logger.info(exit_msg)

    return program_status


if __name__ == "__main__":
    main()
