''' this script reads analyte agilent size values stored in a csv file generated by
    another script, updates the Qubit molarity, molarity dilution, DNA needed and
    water needed values '''

import os
import csv
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_group_id,
                            get_analyte_name,
                            get_sample_concentration,
                            get_data_filename,
                            archive_file,
                            SampleValueDataReader,
                            QCFlagsReaderWriter,
                            ConcentrationWriter,
                            ProcessingError)

class SampleDataReader:
    ''' read data from the csv instrument data file '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.filename = None
        self.sample_concs_map = {}

        process_type = "QBT2C"
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
        sample_concentration_idx = None
        for idx, field in enumerate(field_names):
            self.logger.debug("idx: {}, field: {}".format(idx, field))
            lfield = field.strip().lower()
            if lfield == "name":
                sample_name_idx = idx
            if lfield == "stock conc.":
                sample_concentration_idx = idx
            elif sample_name_idx and sample_concentration_idx:
                break

        if sample_name_idx != 0 and sample_name_idx is None:
            err_msg = "Expected 'Name' field missing in the Qubit data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if sample_concentration_idx != 0 and sample_concentration_idx is None:
            err_msg = "Expected 'Stock Conc.' field missing in the Qubit2 data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (sample_name_idx, sample_concentration_idx)

    def extract_data(self):
        ''' read sample/concentration data from the csv data file '''
        conc_data = []
        self.logger.info("Reading concentrations from the data file ...")

        if not self.filename:
            err_msg = "Error getting daa file name"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        try:
            with open(self.filename, 'r') as file_descriptor:
                conc_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading concentration data file - {}".format(str(err))
            self.logger.error(err_msg)
            archive_file(self.filename)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading concentration data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        field_names = list(conc_data[0])
        self.logger.debug(field_names)

        (sample_name_idx, sample_concentration_idx) = self.get_field_indices(field_names)

        for data in conc_data[1:]:
            sample_name = (data[sample_name_idx]).strip()
            if not sample_name:
                err_msg = "Invalid Test Name: {}".format(sample_name)
                self.logger.error(err_msg)
                archive_file(self.filename)
                raise ProcessingError(err_msg)
            conc = float(data[sample_concentration_idx])
            if conc <= 0:
                err_msg = ("Invalid concentration value: {} for Sample name: {}"
                           .format(conc, sample_name))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            if sample_name not in self.sample_concs_map:
                self.sample_concs_map[sample_name] = conc


        for name, conc in self.sample_concs_map.items():
            self.logger.debug("sample_name: {0:>8}, sample concentration: {1:>5.1f}"
                              .format(name, conc))

        archive_file(self.filename)

class QubitConcentrationUpdaterMgr:
    ''' update the analyte concentrations using REST API '''
    def __init__(self, logger, gau_interface, use_mode,
                 step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.group_id = group_id
        self.use_mode = use_mode
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []
        self.process_dom = None

        self.sd_reader = SampleDataReader(logger, group_id)
        self.sd_reader.extract_data()
        if group_id == "NGS":
            self.asd_reader = SampleValueDataReader(logger, group_id, "AgilentSizes.dat")
            self.asd_reader.extract_data()
        elif group_id == "CTG":
            self.qc_flag_reader = QCFlagsReaderWriter(logger, gau_interface, group_id)
            self.conc_writer = ConcentrationWriter(logger, gau_interface, group_id)

    def build_analyte_uri_list(self):
        ''' build a list of analyte URIs '''
        self.logger.info("Building the artifacts list ...")
        process_xml = self.gau_interface.getResourceByURI(self.process_uri)
        self.process_dom = parseString(process_xml)
        self.logger.debug(self.process_dom.toprettyxml())

        ioutput_elements = self.process_dom.getElementsByTagName("input-output-map")
        for ioutput_elem in ioutput_elements:
            output_elem = ioutput_elem.getElementsByTagName("output")[0]
            if output_elem:
                output_elem_type = output_elem.getAttribute("output-type")
                output_elem_generation_type = output_elem.getAttribute("output-generation-type")

                if (output_elem_generation_type == "PerInput" and
                        ((output_elem_type == "Analyte" and  self.use_mode == "5nM") or
                         (output_elem_type == "ResultFile" and self.use_mode == "Conc"))):
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def update_qubit_udfs(self):
        ''' update the analyte concentrations, average concentration and qc flag '''

        if self.group_id == "CTG":
            self.qc_flag_reader.read_qc_flags()

        for analyte_uri in self.analyte_uri_list:
            self.logger.debug("Analyte URI: {}".format(analyte_uri))
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)

            try:
                #sample_name = get_sample_name(self.gau_interface, analyte_dom)
                analyte_name = get_analyte_name(analyte_dom)
                self.logger.debug("analyte_name: {}".format(analyte_name))
                self.logger.debug(analyte_dom.toprettyxml())

                conc = get_sample_concentration(analyte_name, self.sd_reader.sample_concs_map)
                self.logger.debug("Updating {} concentrations ...".format(analyte_name))

                if self.use_mode == "Conc":
                    self.logger.debug("Updating {} concentrations ... 1")
                    qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                    if not qc_flag_elem:
                        err_msg = "Cannot find QC Flag node for " + analyte_name
                        self.logger.debug(err_msg)
                        raise ProcessingError(err_msg)
                    self.logger.debug("Updating {} concentrations ... 2t_molarity")
                    if self.group_id == "CTG":
                        self.conc_writer.add_conc_value(analyte_name, conc)
                    if conc > 15.0:
                        qc_flag_elem.firstChild.replaceWholeText("PASSED")
                        if self.group_id == "CTG":
                            self.qc_flag_reader.add_qc_flag(analyte_name, "PASSED")
                    else:
                        qc_flag_elem.firstChild.replaceWholeText("FAILED")
                        if self.group_id == "CTG":
                            self.qc_flag_reader.add_qc_flag(analyte_name, "Failed")

                self.logger.debug("Updating {} concentrations ... 3")
                self.gau_interface.setUDF(analyte_dom, "Concentration (ng/ul)",
                                          conc,
                                          "Numeric")

                if self.use_mode == "5nM":
                    agilent_size = self.asd_reader.get_agilent_size_value(analyte_name)
                    if not agilent_size:
                        err_msg = "Cannot find Agilent Size value for " + analyte_name
                        self.logger.debug(err_msg)
                        raise ProcessingError(err_msg)
                    agilent_size = float(agilent_size)
                    self.gau_interface.setUDF(analyte_dom, "Agilent Size",
                                              agilent_size,
                                              "Numeric")

                    qubit_molarity = 1000000 * conc/(660 * agilent_size)
                    self.gau_interface.setUDF(analyte_dom, "Qubit Molarity (nM)",
                                              qubit_molarity,
                                              "Numeric")

                    dilution = float(self.gau_interface.getUDF(self.process_dom, "Dilution (nM)"))
                    if not dilution:
                        err_msg = "Cannot find Dilution value"
                        self.logger.debug(err_msg)
                        raise ProcessingError(err_msg)
                    self.logger.debug("{}  ----  {}".format(agilent_size, dilution))
                    dilution = float(dilution)
                    molarity_dilution = qubit_molarity/dilution
                    self.gau_interface.setUDF(analyte_dom, "Molarity Dilution",
                                              molarity_dilution,
                                              "Numeric")

                    dna_needed = 100/molarity_dilution
                    self.gau_interface.setUDF(analyte_dom, "DNA needed (ul)",
                                              dna_needed,
                                              "Numeric")

                    water_needed = 100 - dna_needed
                    self.gau_interface.setUDF(analyte_dom, "Water needed (ul)",
                                              water_needed,
                                              "Numeric")

                self.logger.debug("Updating {} concentrations ... 4")

                rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                self.logger.debug("Updating {} concentrations ... 5")
                self.logger.debug(rsp_xml)
            except Exception as err:
                err_msg = "Error updating concencentations, " + str(err)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            #rsp_dom = parseString(rsp_xml)
            self.logger.debug("Updating {} concentrations ... 6")
            #self.logger.debug(rsp_dom.toprettyxml())
            self.logger.debug("Updating {} concentrations ... 7")

        if self.group_id == "CTG":
            self.qc_flag_reader.save_qc_flags()
            self.conc_writer.save_conc_values()
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
        exit_msg = None
        qcu_mgr = QubitConcentrationUpdaterMgr(logger, gau_interface, args.useMode,
                                               args.stepURI, args.groupId)
        qcu_mgr.build_analyte_uri_list()
        qcu_mgr.update_qubit_udfs()
    except ProcessingError as err:
        exit_msg = "Qubit concentration load failed - {}".format(str(err))
        logger.error(exit_msg)
        program_status = 1
    except Exception as err:
        exit_msg = "Qubit concentration load failed - {}".format(str(err))
        program_status = 1
        logger.error(exit_msg)
    else:
        exit_msg = "Qubit concentration load completed successfully"
        program_status = 0
        logger.info(exit_msg)
    finally:
        print(exit_msg)

    return program_status


if __name__ == "__main__":
    main()
