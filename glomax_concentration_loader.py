''' this script reads concentrations stored in a csv file generated from
    a Glomax instrument and updates the sample concentration '''

import csv
import glob
import os
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from well_plate_layout_96 import WELL_PLATE_LAYOUT96
from gls_func_utils import (log_cmdline_args,
                            get_logger,
                            validate_well_location,
                            validate_layout_type,
                            get_layout_filename,
                            validate_group_id,
                            make_search_directory,
                            archive_file,
                            ProcessingError
                           )

class ConcentrationDataReader:
    ''' read concentration data from cvs files '''
    def __init__(self, logger, group_id):
        self.logger = logger
        self.layout_filename = None
        self.glomax_1_3_filename = None  # csv file name
        self.glomax_1_10_filename = None
        self.plate_layout_map_type_1_3 = {}
        self.plate_layout_map_type_1_10 = {}
        self.concentration_map_1_3 = {}
        self.concentration_map_1_10 = {}

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        try:
            self.layout_filename = get_layout_filename("GLMXL", group_id)
        except ProcessingError as err:
            err_msg = "Missing plate layout mapping file - " + str(err)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        process_type = "GLMXC"
        search_dir = make_search_directory(process_type, group_id)
        for fname in glob.glob(search_dir):
            base_filename = os.path.basename(fname)
            lfname = base_filename.lower()
            if process_type.lower() in lfname:
                if group_id.lower() in lfname:
                    if "1_3" in lfname:
                        self.glomax_1_3_filename = fname
                    elif "1_10" in lfname:
                        self.glomax_1_10_filename = fname
                    if self.glomax_1_3_filename and self.glomax_1_10_filename:
                        break

        if not self.glomax_1_3_filename or not self.glomax_1_10_filename:
            msg = "Missing concentration data file"
            logger.error(msg)
            raise ProcessingError(msg)

    def extract_layout_data(self):
        ''' read the data into dictionnaries '''
        layout_data = []
        self.logger.debug("Reading layout data file ...")

        try:
            with open(self.layout_filename, 'r') as file_descriptor:
                layout_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading platelayout data file - {}".format(str(err))
            self.logger.error(err_msg)
            archive_file(self.glomax_1_3_filename)
            archive_file(self.glomax_1_10_filename)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading platelayout data file - {}".format(str(err))
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)


        for data in layout_data[1:]:
            (source_well, destination_well, well_type) = data
            validate_well_location("96", source_well)
            validate_well_location("96", destination_well)

            source_well = WELL_PLATE_LAYOUT96[source_well]
            destination_well = WELL_PLATE_LAYOUT96[destination_well]

            if "1-10" in well_type:
                self.plate_layout_map_type_1_10[source_well] = destination_well
            elif "1-3" in well_type:
                self.plate_layout_map_type_1_3[source_well] = destination_well
            else:
                raise ProcessingError("Error reading Glomax layout mapping")

        for key, value in self.plate_layout_map_type_1_10.items():
            self.logger.debug("key: {}, value: {}".format(key, value))
        self.logger.debug("=================================================================\n")
        for key, value in self.plate_layout_map_type_1_3.items():
            self.logger.debug("key: {}, value {}".format(key, value))

    def extract_concentration_data(self):
        ''' read the concentration data from the cvs file into dictionnaries '''
        self.logger.debug("Reading concentration data file ...")
        conc_data = []
        if self.glomax_1_3_filename:
            try:
                with open(self.glomax_1_3_filename, 'r') as file_descriptor:
                    conc_data = list(csv.reader(file_descriptor))
            except IOError as err:
                err_msg = ("Error reading Glomax 1-3 concentration csv data file - {}"
                           .format(str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)
            except Exception as err:
                archive_file(self.glomax_1_3_filename)
                archive_file(self.glomax_1_10_filename)
                err_msg = ("Error reading Glomax 1-3 concentration csv data file - {}"
                           .format(str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

        for data in conc_data:
            if len(data) >= 5:
                first_field_value = data[0].strip()
                fifth_field_value = data[4].strip()
                if "n/a".lower() not in fifth_field_value.lower():
                    if first_field_value in WELL_PLATE_LAYOUT96:
                        well_location = WELL_PLATE_LAYOUT96[first_field_value]
                        concentration_value = fifth_field_value
                        self.concentration_map_1_3[well_location] = concentration_value
                        self.logger.debug("well_location: {}, concentration_value: {}"
                                          .format(well_location, concentration_value))

        if self.glomax_1_10_filename:
            try:
                with open(self.glomax_1_10_filename, 'r') as file_descriptor:
                    conc_data = list(csv.reader(file_descriptor))
            except IOError as err:
                err_msg = ("Error reading Glomax 1-10 concentration csv data file - {}"
                           .format(str(err)))
                self.logger.error(err_msg)
                archive_file(self.glomax_1_3_filename)
                archive_file(self.glomax_1_10_filename)
                raise ProcessingError(err_msg)
            except Exception as err:
                err_msg = ("Error reading Glomax 1-3 concentration csv data file - {}"
                           .format(str(err)))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

        self.logger.debug("======================================================================")

        for data in conc_data:
            if len(data) >= 5:
                first_field_value = data[0].strip()
                fifth_field_value = data[4].strip()
                if "n/a".lower() not in fifth_field_value.lower():
                    if first_field_value in WELL_PLATE_LAYOUT96:
                        well_location = WELL_PLATE_LAYOUT96[first_field_value]
                        concentration_value = fifth_field_value
                        self.concentration_map_1_10[well_location] = concentration_value
                        self.logger.debug("well_location: {}, concentration_value: {}"
                                          .format(well_location, concentration_value))

        archive_file(self.glomax_1_3_filename)
        archive_file(self.glomax_1_10_filename)

class GlomaxConcentrationLoaderMgr:
    ''' handles the updating of the analyte concentration read from data file '''
    def __init__(self, logger, gau_interface, step_uri, layout_type, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uris = []

        status = validate_layout_type(layout_type)
        if not status:
            err_msg = "Invalid layout type - {}".format(layout_type)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        self.layout_type = layout_type.lower()

        self.conc_data_reader = ConcentrationDataReader(self.logger, group_id)
        self.conc_data_reader.extract_layout_data()
        self.conc_data_reader.extract_concentration_data()

    def build_list_of_analyte_uris(self):
        ''' make up a list of analytes contained in the process xml load '''
        self.logger.info("Building the artifacts list ...")
        process_xml = self.gau_interface.getResourceByURI(self.process_uri)
        process_dom = parseString(process_xml)
        self.logger.debug(process_dom.toprettyxml())

        ioutput_elements = process_dom.getElementsByTagName("input-output-map")
        for ioutput_elem in ioutput_elements:
            output_elem = ioutput_elem.getElementsByTagName("output")[0]
            output_elem_type = output_elem.getAttribute("output-type")
            output_elem_generation_type = output_elem.getAttribute("output-generation-type")
            if output_elem_type == "ResultFile" and output_elem_generation_type == "PerInput":
                analyte_uri = output_elem.getAttribute("uri")
                self.analyte_uris.append(analyte_uri)
                self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uris:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def get_container_name(self, analyte_dom):
        ''' extract the container name from the analyte xml load '''
        container_name = None
        elems = analyte_dom.getElementsByTagName("container")
        if elems:
            container_uri = elems[0].getAttribute("uri")
            if container_uri:
                container_xml = self.gau_interface.getResourceByURI(container_uri)
                container_dom = parseString(container_xml)

                name_elem = container_dom.getElementsByTagName("name")
                if name_elem:
                    container_name = name_elem[0].firstChild.data
                else:
                    raise ProcessingError("Cannot find name node in container")
            else:
                raise ProcessingError("Cannot find container uri")
        else:
            raise ProcessingError("Cannot find container node in analyte")

        return container_name

    def update_concentrations(self):
        ''' update the analyte concentration and QC flag via the REST API '''
        self.logger.info("Updating Glomax concentrations ...")
        for analyte_uri in self.analyte_uris:
            self.logger.debug("Analyte URI: {0}".format(analyte_uri))
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)
            self.logger.debug(analyte_dom.toprettyxml())

            container_name = self.get_container_name(analyte_dom)
            self.logger.debug("container_name: {}".format(container_name))

            # get the well location for this analyte
            well_elem = analyte_dom.getElementsByTagName("value")[0]
            well_location = well_elem.firstChild.data
            self.logger.debug("Analyte URI: {}\nwell_location: {}"
                              .format(analyte_uri, well_location))

            if self.layout_type == "PLNMNL".lower():
                if well_location in self.conc_data_reader.plate_layout_map_type_1_10:
                    well_location = self.conc_data_reader.plate_layout_map_type_1_10[well_location]
                    container_name = "Glomax 1-10"
                elif well_location in self.conc_data_reader.plate_layout_map_type_1_3:
                    well_location = self.conc_data_reader.plate_layout_map_type_1_3[well_location]
                    container_name = "Glomax 1-3"
                else:
                    err_msg = ("Cannot map well location: ({}) "
                               .format(well_location))
                    self.logger.error(err_msg)
                    raise ProcessingError(err_msg)

            if (well_location not in self.conc_data_reader.concentration_map_1_10 and
                    well_location not in self.conc_data_reader.concentration_map_1_3):
                err_msg = ("Missing concentration data for well location: ({}) "
                           .format(well_location))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            well_concentration = (self.conc_data_reader.concentration_map_1_10[well_location] if
                                  container_name == "Glomax 1-10" else
                                  self.conc_data_reader.concentration_map_1_3[well_location])
            self.logger.debug("Updating {} concentration ...".format(well_location))

            # set the QC flag for this analyte
            if self.layout_type == "GLMXL".lower():
                qc_flag_elem = analyte_dom.getElementsByTagName("qc-flag")[0]
                if float(well_concentration) > 10.0:
                    qc_flag_elem.firstChild.replaceWholeText("PASSED")
                else:
                    qc_flag_elem.firstChild.replaceWholeText("FAILED")

            # set the concentration for this analyte
            self.gau_interface.setUDF(analyte_dom, "Concentration (ng/ul)",
                                      well_concentration, "Numeric")
            try:
                rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                rsp_dom = parseString(rsp_xml)
                self.logger.debug(rsp_dom.toprettyxml())
            except Exception as err:
                msg = "Error updating concentration for analyte -  {}".format(str(err))
                raise ProcessingError(msg)

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-l", "--layoutType", help="the layout type (Default, EndRepair...)")
    parser.add_argument("-g", "--groupId", help="the work group that run this script")
    args = parser.parse_args()

    tokens = args.stepURI.split("/")
    hostname = "/".join(tokens[2:3])

    prog_name = os.path.basename(__file__)
    logger = get_logger(prog_name)

    log_cmdline_args(logger, prog_name, hostname, args.username, args.password, args.stepURI)

    # initiliase REST API interface
    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    program_status = None
    exit_msg = None
    try:
        glomax_conc_loader_mgr = GlomaxConcentrationLoaderMgr(logger, gau_interface, args.stepURI,
                                                              args.layoutType, args.groupId)
        glomax_conc_loader_mgr.build_list_of_analyte_uris()
        glomax_conc_loader_mgr.update_concentrations()
    except ProcessingError as perr:
        exit_msg = "Glomax concenration load failed - {}\n".format(str(perr))
        logger.error(exit_msg)
        program_status = 1
    except Exception as err:
        exit_msg = "Glomax concenration load failed - {}\n".format(str(err))
        logger.error(exit_msg)
        program_status = 1
    else:
        exit_msg = "Glomax concenration load completed successfully"
        logger.info(exit_msg)
        program_status = 0
    finally:
        print(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
