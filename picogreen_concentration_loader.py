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
                            get_data_filename,
                            set_exit_status,
                            archive_file,
                            QCFlagsReaderWriter,
                            ConcentrationWriter,
                            ProcessingError)
class SampleDataReader:
    ''' read concentration data from the csv data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_concs_map = {}

        process_type = "PCGRNC"
        group_id = group_id.strip()

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.filename = get_data_filename(process_type, group_id)

    def extract_data(self):
        ''' read concentration data from the csv data file '''
        conc_data = []
        self.logger.info("Reading concentrations from the data file ...")

        try:
            with open(self.filename, 'r') as file_descriptor:
                conc_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading Picogreen concentration data file - {}".format(str(err))
            archive_file(self.filename)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading Picogreen concentration data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)


        for idx, data in enumerate(conc_data):
            if "unk" in data[0].lower():
                sample_name = data[1]
                if sample_name:
                    if sample_name not in self.sample_concs_map:
                        self.sample_concs_map[sample_name] = []
                    self.sample_concs_map[sample_name].append(data[12])
                    self.sample_concs_map[sample_name].append(data[15])
                    if idx != 0:
                        self.sample_concs_map[sample_name].append(data[29])
                        self.sample_concs_map[sample_name].append(data[30])
                    else:
                        self.sample_concs_map[sample_name].append(data[16])
                        self.sample_concs_map[sample_name].append(data[17])

        for name, values in self.sample_concs_map.items():
            self.logger.debug("sample: {}, values: {} ".format(name, values))

        archive_file(self.filename)


class PicogreenConcentrationUpdaterMgr:
    ''' update the analyte concentrations using REST API '''
    def __init__(self, logger, gau_interface, step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []
        self.sample_qc_flag_map = {}

        self.sde_reader = SampleDataReader(logger, group_id)
        self.sde_reader.extract_data()
        self.qc_flag_reader = QCFlagsReaderWriter(logger, gau_interface, group_id)
        self.conc_writer = ConcentrationWriter(logger, gau_interface, group_id)

    def get_sample_concentration(self, analyte_name):
        ''' get the concentation value from the analyte/conc map '''
        if analyte_name in self.sde_reader.sample_concs_map:
            return (self.sde_reader.sample_concs_map[analyte_name][0],
                    self.sde_reader.sample_concs_map[analyte_name][1])

        for sample, values in self.sde_reader.sample_concs_map.items():
            if (analyte_name in sample) or (sample in analyte_name):
                return (float(values[0]), float(values[1]))

        err_msg = "Missing concentration data for sample: {} ".format(analyte_name)
        self.logger.error(err_msg)
        raise ProcessingError(err_msg)

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
                if output_type == "ResultFile":
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def update_qpcr_udfs(self):
        ''' update the analyte concentrations, average concentration and qc flag '''
        self.qc_flag_reader.read_qc_flags()
        for analyte_uri in self.analyte_uri_list:
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)
            self.logger.debug("=================== ANALYTE DOM ==================================")
            self.logger.debug(analyte_dom.toprettyxml())

            try:
                sample_name = get_sample_name(self.gau_interface, analyte_dom)
                analyte_name = get_analyte_name(analyte_dom)

                if sample_name not in self.sde_reader.sample_concs_map:
                    sample_name = analyte_name
                self.logger.debug("Updating {} concentrations ...".format(sample_name))

                (conc, initial_volume) = self.get_sample_concentration(analyte_name)

                conc = float(conc)
                initial_volume = float(initial_volume)

                qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                if float(conc) >= 30:
                    qc_flag_elem.firstChild.replaceWholeText("PASSED")
                    self.qc_flag_reader.add_qc_flag(analyte_name, "PASSED")
                else:
                    qc_flag_elem.firstChild.replaceWholeText("FAILED")
                    self.qc_flag_reader.add_qc_flag(analyte_name, "FAILED")

                self.gau_interface.setUDF(analyte_dom, "Concentration",
                                          conc,
                                          "Numeric")

                self.conc_writer.add_conc_value(analyte_name, conc)
                self.gau_interface.setUDF(analyte_dom, "Initial volume (ul)",
                                          initial_volume,
                                          "Numeric")

                if conc >= 250:
                    required_concentration = 250
                elif conc > 125:
                    required_concentration = 125
                else:
                    required_concentration = conc

                self.gau_interface.setUDF(analyte_dom, "Required Concentration",
                                          required_concentration,
                                          "Numeric")

                final_volume = (conc * initial_volume)/required_concentration
                self.gau_interface.setUDF(analyte_dom, "Final volume (ul)",
                                          final_volume,
                                          "Numeric")
                self.conc_writer.add_conc_value(analyte_name, final_volume)

                vol_te_added = final_volume - initial_volume
                self.gau_interface.setUDF(analyte_dom, "TE needed (ul)",
                                          vol_te_added,
                                          "Numeric")

                self.gau_interface.updateObject(analyte_dom, analyte_uri)
            except Exception as err:
                err_msg = ("Error updating sample '{}' concentration: - {} "
                           .format(analyte_name, str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

        self.qc_flag_reader.save_qc_flags()
        self.conc_writer.save_conc_values()

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation and start processing '''
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
        exit_status = None
        qcu_mgr = PicogreenConcentrationUpdaterMgr(logger, gau_interface,
                                                   args.stepURI, args.groupId)
        qcu_mgr.build_analyte_uri_list()
        qcu_mgr.update_qpcr_udfs()
    except ProcessingError as err:
        exit_msg = "Picogreen concentration load failed - {}".format(str(err))
        exit_status = "ERROR"

        program_status = 1
        logger.error(exit_msg)
    except Exception as err:
        exit_msg = "Picogreen concentration load failed - {}".format(str(err))
        exit_status = "ERROR"
        program_status = 1
        logger.error(exit_msg)
    else:
        exit_msg = "Picogreen concentration load completed successfully"
        exit_status = "OK"
        program_status = 0
        logger.info(exit_msg)
    finally:
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        print(exit_msg)

    return program_status


if __name__ == "__main__":
    main()
