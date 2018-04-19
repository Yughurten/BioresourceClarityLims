''' places samples from a 96 plate layout using a well placement layout file '''

import csv
import os
import argparse
from xml.dom.minidom import parseString

from well_plate_layout_96 import WELL_PLATE_LAYOUT96
from well_plate_layout_384 import WELL_PLATE_LAYOUT384
from default_layout_info import DEFAULT_LAYOUT_INFO
from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            validate_well_location,
                            validate_layout_type,
                            validate_group_id,
                            get_layout_filename,
                            get_sample_name,
                            set_exit_status,
                            get_error_message,
                            log_cmdline_args,
                            ProcessingError)

class PlateLayoutReader:
    ''' reads mapping data from a platelayout csv data file '''
    def __init__(self, logger, layout_type, group_id):
        self.logger = logger
        self.playout_map = {}
        self.layout_type = layout_type
        self.layout_filename = None

        layout_type = layout_type.strip()
        group_id = group_id.strip()

        status = validate_layout_type(layout_type)
        if not status:
            err_msg = "Invalid layout type - {}".format(layout_type)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        self.layout_type = layout_type

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.build_data_filename(layout_type, group_id)

    def build_data_filename(self, layout_type, group_id):
        ''' make data file name by using a search path '''
        try:
            self.filename = get_layout_filename(layout_type, group_id)
        except ProcessingError as err:
            err_msg = "Missing plate layout mapping file - " + str(err)
            self.logger.error(err_msg)
            raise ProcessingError("Missing plate layout mapping file")

    def extract_data(self):
        ''' read the csv data file into a platelayout mapping dict '''
        layout_data = []
        self.logger.debug("Reading plate layout")
        try:
            with open(self.filename, 'r') as file_descriptor:
                layout_data = list(csv.reader(file_descriptor))
        except IOError as err:
            err_msg = "Error reading platelayout data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error reading platelayout data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        csv_source_idx = -1
        dest_well_idx = -1
        header_row_idx = 0
        for data_idx, data in enumerate(layout_data):
            field_names = list(data)
            self.logger.debug("data_idx: {}, field_names: {}".format(data_idx, field_names))
            for field_idx, field in enumerate(field_names):
                #self.logger.debug("field_idx {}, field: {}".format(field_idx, field))
                lfield = field.strip().lower()
                self.logger.debug("lfield: " + lfield)
                if lfield == "source well" or lfield == "adapter":
                    csv_source_idx = field_idx
                elif lfield == "destination well":
                    dest_well_idx = field_idx

            if csv_source_idx != -1 and dest_well_idx != -1:
                header_row_idx = data_idx
                break

        if csv_source_idx == -1:
            err_msg = "Expected 'Source Well/Adapter' field missing in the layout data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if dest_well_idx == -1:
            err_msg = "Expected 'Destination Well' field missing in the layout data file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        plate_type = DEFAULT_LAYOUT_INFO[self.layout_type][0]
        self.logger.debug("plate_type: {}".format(plate_type))

        self.logger.debug("header_row_idx: {}".format(header_row_idx))
        for idx, data in enumerate(layout_data):
            self.logger.debug("header_row_idx: {}, idx: {}".format(header_row_idx, idx))
            if idx <= header_row_idx:
                continue
            csv_source = data[csv_source_idx]
            source_name = None
            if csv_source[1:].isdigit():
                validate_well_location("96", csv_source)
                source_name = WELL_PLATE_LAYOUT96[csv_source]
            else: #qPCR uses the sample name as the source well location
                source_name = csv_source
            dest_well = data[dest_well_idx]
            #self.logger.debug("source: {}, dest_well: {}".format(source, dest_well))
            well_type = "96" if "96" in plate_type else "384"
            #self.logger.debug("dest_well: {}, well_type: {}".format(dest_well, well_type))
            validate_well_location(well_type, dest_well)
            dest_plate_location = (WELL_PLATE_LAYOUT96[dest_well]
                                   if "96" in plate_type
                                   else WELL_PLATE_LAYOUT384[dest_well])

            if source_name not in self.playout_map:
                self.playout_map[source_name] = []
            self.logger.debug("source_name: {}, dest_plate_location: {}"
                              .format(source_name, dest_plate_location))
            self.playout_map[source_name].append(dest_plate_location)

        for key, value in self.playout_map.items():
            self.logger.debug("k: {}, v: {}".format(key, value))


class DefaultSampleAutoPlacerMgr:
    ''' use the platelayout read from a csv data file to perform sample auto placement '''
    def __init__(self, logger, gau_interface, step_uri, layout_type, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.step_uri = step_uri
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        self.lims_id = tokens[6]
        self.layout_type = layout_type

        self.artifact_list = []
        self.artifacts_dom = None
        self.src_2_dest_map = {}   #a mapping of inputs to their destinations
        self.well_map = {}
        self.reagents_map = {}

        self.playout_reader = PlateLayoutReader(logger, layout_type, group_id)
        self.playout_reader.extract_data()

    def get_step_configuration(self):
        ''' get the step configuration xml load '''

        step_xml = self.gau_interface.getResourceByURI(self.step_uri)
        step_dom = parseString(step_xml)
        elems = step_dom.getElementsByTagName("configuration")

        configuration_xml = None
        if elems:
            configuration_xml = elems[0].toxml()

        return configuration_xml

    def cache_artifact(self, lims_id):
        ''' store the lims_id in a list for later use '''
        if lims_id not in self.artifact_list:
            self.artifact_list.append(lims_id)

    def prepare_cache(self):
        ''' build the link xml to be use later '''
        link_xml = '<ri:links xmlns:ri="http://genologics.com/ri">'

        for lims_id in self.artifact_list:
            link = '<link uri="' + self.base_uri + 'artifacts/' + lims_id
            link += '" rel="artifacts"/>'
            link_xml += link
        link_xml += '</ri:links>'

        #link_dom = parseString(link_xml)
        #self.logger.debug("link_dom: {}".format(link_dom.toprettyxml()))

        try:
            link_uri = self.base_uri + "artifacts/batch/retrieve"
            artifact_xml = self.gau_interface.getBatchResourceByURI(link_uri, link_xml)
            self.artifacts_dom = parseString(artifact_xml)
        except Exception as err:
            self.logger.error(str(err))
            raise Exception("Error error getting artifcats")

    def get_artifact(self, lims_id):
        '''  given a lims id return its artifact info '''
        artifact = None

        elms = self.artifacts_dom.getElementsByTagName("art:artifact")
        for artifact_elem in elms:
            container_lims_id = artifact_elem.getAttribute("limsid")
            if container_lims_id == lims_id:
                artifact = artifact_elem
        if not artifact:
            err_msg = "Cannot find artifact ({}) in artifact cache".format(lims_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return artifact

    def create_container(self):
        ''' given a container type create a container '''
        self.logger.debug("create_container() called ...")

        container_type_name = DEFAULT_LAYOUT_INFO[self.layout_type][0]
        container_type = DEFAULT_LAYOUT_INFO[self.layout_type][1]

        container_xml = '<?xml version="1.0" encoding="UTF-8"?>'
        container_xml += ('<con:container xmlns:con="http://genologics.com/ri/container"'
                          + ' xmlns:udf="http://genologics.com/ri/userdefined">')
        container_xml += '<name>' + self.layout_type + '</name>'
        container_xml += '<udf:field type="Text" name="Freezer box">31A</udf:field>'
        container_xml += '<udf:field type="Text" name="Box location">1</udf:field>'
        container_xml += ('<type uri="' + self.base_uri + 'containertypes/'
                          + container_type + '" name="' + container_type_name + '"/>')
        container_xml += '</con:container>'

        container_dom = parseString(container_xml)
        #self.logger.debug(container_dom.toprettyxml())

        container_uri = self.base_uri + "containers"

        try:
            container_xml = self.gau_interface.createObject(container_xml, container_uri)
        except Exception as err:
            err_msg = "Cannot create a container - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        container_dom = parseString(container_xml)
        #self.logger.debug(container_dom.toprettyxml())
        container_lims_id = None
        elems = container_dom.getElementsByTagName("con:container")
        if elems:
            container_lims_id = elems[0].getAttribute("limsid")

        if not container_lims_id:
            err_msg = "Cannot find LIMS id of container"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return container_lims_id

    def get_source_well_location(self, source_dom, source_lims_id):
        ''' get the well position for the input artifact '''
        src_well_lctn = None
        self.logger.debug("====================  ANALYTE DOM ================================")
        self.logger.debug(source_dom.toprettyxml())
        elems = source_dom.getElementsByTagName("value")
        if elems:
            src_well_lctn = self.gau_interface.getInnerXml(elems[0].toxml(), "value")
        else:
            raise ProcessingError("Cannot find location in well location elem")
        if ":" not in src_well_lctn:
            err_msg = ("Unable to determine well placement for artifact {}"
                       .format(source_lims_id))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return src_well_lctn

    def build_source_to_destination_map(self):
        ''' build source to destination lims ids map '''
        ## step one: get the process XML
        process_uri = self.base_uri + "processes/" + self.lims_id
        process_xml = self.gau_interface.getResourceByURI(process_uri)
        process_dom = parseString(process_xml)

        self.logger.debug(process_dom.toprettyxml())

        source_destination_maps = process_dom.getElementsByTagName("input-output-map")

        for source_destination_map in source_destination_maps:
            destination = source_destination_map.getElementsByTagName("output")
            destination_type = destination[0].getAttribute("output-type")
            dest_generation_type = destination[0].getAttribute("output-generation-type")
            artifact_type = "ResultFile" if self.layout_type in ["qPCRL", "PLNMNL"] else "Analyte"
            if destination_type == artifact_type and dest_generation_type == "PerInput":
                destination_lims_id = destination[0].getAttribute("limsid")
                self.cache_artifact(destination_lims_id)

                nodes = source_destination_map.getElementsByTagName("input")
                source_lims_id = nodes[0].getAttribute("limsid")
                self.cache_artifact(source_lims_id)

                ## create a map entry
                if not source_lims_id in self.src_2_dest_map:
                    self.src_2_dest_map[source_lims_id] = []

                self.logger.debug("source_lims_id: {} destination_lims_id: {}"
                                  .format(source_lims_id, destination_lims_id))
                self.src_2_dest_map[source_lims_id].append(destination_lims_id)

        if not self.src_2_dest_map:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        for key, value in self.src_2_dest_map.items():
            self.logger.debug("key: {} value: {}\n".format(key, value))


    def auto_place(self):
        ''' performs actual automatic placement of samples in the container '''
        self.logger.debug("autoPlace() called ...")

        destination_container = self.create_container()
        self.logger.debug("Container lims_id: {}".format(destination_container))

        self.build_source_to_destination_map()

        ## build the  cache of Analytes
        self.prepare_cache()

        # get the configuration
        step_configuartion = self.get_step_configuration()

        placement_xml = '<?xml version="1.0" encoding="UTF-8"?>'
        placement_xml += ('<stp:placements xmlns:stp="http://genologics.com/ri/step" uri="'
                          + self.step_uri +  '/placements">')
        placement_xml += '<step uri="' + self.step_uri + '"/>'
        placement_xml += step_configuartion
        placement_xml += '<selected-containers>'
        placement_xml += ('<container uri="' + self.base_uri
                          + 'containers/' + destination_container + '"/>')
        placement_xml += '</selected-containers><output-placements>'

        ## let's process our cache, one input at a time
        placement_list = []
        for source_lims_id in self.src_2_dest_map:
            source_dom = self.get_artifact(source_lims_id)
            src_well_lctn = self.get_source_well_location(source_dom, source_lims_id)
            dest_well_idx = 0
            self.logger.debug("source_lims_id: {}, src_well_lctn: {}"
                              .format(source_lims_id, src_well_lctn))
            destination_lims_ids = self.src_2_dest_map[source_lims_id]
            self.logger.debug("source_lims_id: {} destination_lims_ids: {}"
                              .format(source_lims_id, str(destination_lims_ids)))
            for destination in destination_lims_ids:
                self.logger.debug("====> 1")
                destination_dom = self.get_artifact(destination)
                destination_uri = destination_dom.getAttribute("uri")
                self.logger.debug("====> 2")
                self.logger.debug("========================>  destination {}". format(destination))
                dest_well_lctn = None
                if src_well_lctn in self.playout_reader.playout_map:
                    self.logger.debug(src_well_lctn)
                    self.logger.debug(dest_well_idx)
                    dest_well_lctn = self.playout_reader.playout_map[src_well_lctn][dest_well_idx]
                    self.logger.debug("====> 3")
                elif self.layout_type == "qPCRL":
                    self.logger.debug("==>  well_location {}". format(src_well_lctn))
                    sample_name = get_sample_name(self.gau_interface, source_dom)
                    dest_well_lctn = self.playout_reader.playout_map[sample_name][dest_well_idx]
                    self.logger.debug("====> 4")
                else:
                    err_msg = "Cannot find destination location in map for soure well "
                    err_msg += src_well_lctn
                    raise ProcessingError(err_msg)

                self.logger.debug("====> 5")
                dest_well_idx += 1
                if (src_well_lctn, dest_well_lctn) not in placement_list:
                    pl_xml = '<output-placement uri="' + destination_uri + '">'
                    pl_xml += ('<location><container uri="' + self.base_uri
                               + 'containers/' + destination_container
                               + '" limsid="' + destination_container + '"/>')
                    pl_xml += ('<value>' + dest_well_lctn
                               + '</value></location></output-placement>')

                    placement_xml += pl_xml
                    placement_list.append((src_well_lctn, dest_well_lctn))

        placement_xml += '</output-placements></stp:placements>'

        placement_dom = parseString(placement_xml)

        self.logger.debug(placement_dom.toprettyxml())

        placement_uri = self.step_uri + "/placements"

        rsp_xml = None
        try:
            rsp_xml = self.gau_interface.createObject(placement_xml, placement_uri)
        except Exception as err:
            err_msg = "Cannot perform placement of samples - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if rsp_xml:
            msg = get_error_message(rsp_xml)
            if msg:
                self.logger.error(msg)
                raise ProcessingError(msg)

    def get_artifacts_locations(self):
        ''' get the well locations of artifacts in the placement '''
        self.logger.debug("get_artifacts_locations ...")
        placement_uri = self.step_uri + "/placements"
        placement_xml = self.gau_interface.getResourceByURI(placement_uri)
        placement_dom = parseString(placement_xml)
        self.logger.debug(placement_dom.toprettyxml())
        for artifact in placement_dom.getElementsByTagName("output-placement"):
            artifact_uri = artifact.getAttribute("uri")
            well_location = artifact.getElementsByTagName("value")[0].firstChild.data
            self.well_map[well_location] = artifact_uri


    def build_artifact_reagent_map(self):
        ''' build a map of artifact and associated reagent name '''
        self.logger.debug("build_artifact_reagent_map ...")
        for reagent_name, well_location in self.playout_reader.playout_map.items():
            self.logger.debug(well_location)
            location = well_location[0] # only one location
            if location in self.well_map:
                self.reagents_map[self.well_map[location]] = reagent_name

    def auo_place_reagents(self):
        ''' auto place reagents '''
        self.logger.debug("auto place reagents ...")
        self.get_artifacts_locations()
        self.build_artifact_reagent_map()

        reagents_uri = self.step_uri + "/reagents"

        reagents_xml = self.gau_interface.getResourceByURI(reagents_uri)
        reagents_dom = parseString(reagents_xml)
        self.logger.debug(reagents_dom.toprettyxml())

        reagents_xml = [('<stp:reagents xmlns:stp="http://genologics.com/ri/step" uri="'
                         + self.step_uri + '/reagents">')]
        reagents_xml.append(reagents_dom.getElementsByTagName("step")[0].toxml())
        reagents_xml.append(reagents_dom.getElementsByTagName("configuration")[0].toxml())
        reagents_xml.append(reagents_dom.getElementsByTagName("reagent-category")[0].toxml())
        reagents_xml.append('<output-reagents>')

        output_reagents_node = []
        for artifact_uri, reagent_name in self.reagents_map.items():
            xml = ('<output uri="{}"><reagent-label name="{}"/></output>'
                   .format(artifact_uri, reagent_name))
            output_reagents_node.append(xml)

        reagents_xml.append("".join(output_reagents_node))
        reagents_xml.append('</output-reagents></stp:reagents>')

        reagents_dom = parseString("".join(reagents_xml))
        self.logger.debug(reagents_dom.toprettyxml())

        #rsp_xml = self.gau_interface.createObject("".join(reagents_xml), reagents_uri)
        #self.logger.debug(rsp_xml)

        rsp_xml = None
        try:
            rsp_xml = self.gau_interface.createObject("".join(reagents_xml), reagents_uri)
        except Exception as err:
            err_msg = "Cannot perform auto placement of reagents - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug("1")
        if rsp_xml:
            self.logger.debug(rsp_xml)
            msg = get_error_message(rsp_xml)
            if msg:
                self.logger.error(msg)
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

    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    program_status = 0
    exit_status = None
    exit_msg = None

    try:
        autoplacer_mgr = DefaultSampleAutoPlacerMgr(logger, gau_interface, args.stepURI,
                                                    args.layoutType, args.groupId)
        if args.layoutType == "ADPRL":
            autoplacer_mgr.auo_place_reagents()
        else:
            autoplacer_mgr.auto_place()
    except ProcessingError as perr:
        exit_status = "ERROR"
        if args.layoutType == "ADPRL":
            exit_msg = "Auto-placement of reagents failed - {}".format(str(perr))
        else:
            exit_msg = "Auto-placement of replicates failed - {}".format(str(perr))
        program_status = 1
        logger.error(exit_msg)
    except Exception as err:
        exit_status = "ERROR"
        if args.layoutType == "ADPRL":
            exit_msg = "Auto-placement of reagents failed - {}".format(str(perr))
        else:
            exit_msg = "Auto-placement of replicates failed - {}".format(str(err))
        program_status = 1
        logger.error(exit_msg)
    else:
        exit_status = "OK"
        if args.layoutType == "ADPRL":
            exit_msg = "Auto-placement of reagents completed successfully"
        else:
            exit_msg = "Auto-placement of replicates completed successfully"
        program_status = 0
        logger.info(exit_msg)
    finally:
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        print(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
