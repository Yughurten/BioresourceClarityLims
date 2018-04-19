''' places samples from 2 separate Glomax 2 separate containers
    into a single container using a well placement layout file '''

import csv
import os
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from well_plate_layout_96 import WELL_PLATE_LAYOUT96
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_well_location,
                            validate_layout_type,
                            validate_group_id,
                            get_layout_filename,
                            set_exit_status,
                            get_error_message,
                            ProcessingError
                           )

class PlateLayoutReader:
    ''' plate layout csv data reader '''
    def __init__(self, logger, layout_type, group_id):
        self.logger = logger
        self.playout_type_glmx_1_3 = {}
        self.playout_type_glmx_1_10 = {}
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

        try:
            self.layout_filename = get_layout_filename(layout_type, group_id)
        except ProcessingError as err:
            err_msg = "Missing plate layout mapping file - " + str(err)
            self.logger.error(err_msg)
            raise ProcessingError("Missing plate layout mapping file")

    def extract_layout_data(self):
        ''' reads the layout from the csv idata file '''
        layout_data = []
        try:
            with open(self.layout_filename, 'r') as file_descriptor:
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

        for data in layout_data[1:]:
            (src_well, dest_well, well_type) = data
            validate_well_location("96", src_well)
            validate_well_location("96", dest_well)
            src_well = WELL_PLATE_LAYOUT96[src_well]
            dest_well = WELL_PLATE_LAYOUT96[dest_well]
            self.logger.debug("src_well: {}, dest_well: {}, well_type: {}"
                              .format(src_well, dest_well, well_type))
            try:
                if "1-3" in well_type:
                    if src_well not in self.playout_type_glmx_1_3:
                        #self.playout_type_glmx_1_3[src_well] = []
                        self.playout_type_glmx_1_3[src_well].append(dest_well)
                    #self.playout_type_glmx_1_3[src_well].append("1-10")
                elif "1-10" in well_type:
                    if src_well not in self.playout_type_glmx_1_10:
                        #self.playout_type_glmx_1_10[src_well] = []
                        self.playout_type_glmx_1_10[src_well].append(dest_well)
                    #self.playout_type_glmx_1_10[src_well].append("1-10")
                else:
                    err_msg = "Error in {} layout mapping data".format(self.layout_type)
                    raise ProcessingError(err_msg)
            except Exception as err:
                err_msg = "Error raeding layout file - " + str(err)
                raise ProcessingError(err_msg)

        for key, val in self.playout_type_glmx_1_3.items():
            self.logger.debug("key: {}, val1: {}, val2: {}".format(key, val[0], val[1]))
        self.logger.debug("=================================================================\n")
        for key, val in self.playout_type_glmx_1_10.items():
            self.logger.debug("key: {}, val1: {}, val2: {}".format(key, val[0], val[1]))


class MultiSampleAutoPlacerMgr:
    ''' performs automated placement of samples '''
    def __init__(self, logger, gau_interface, step_uri, layout_type):
        self.logger = logger
        self.gau_interface = gau_interface
        self.step_uri = step_uri
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        self.lims_id = tokens[6]
        self.layout_type = layout_type
        self.artifact_list = []
        self.artifacts_dom = None
        self.src_2_dest_map = {} # a process mapping of inputs to their outputs
        self.layout_extractor = None
        self.dest_flg_1 = None
        self.dest_flg_2 = None

    def extract_layout_data(self, logger, layout_type, group_id):
        ''' calls the csv data file extractor to perform the data extraction '''
        self.layout_extractor = PlateLayoutReader(logger, layout_type, group_id)
        (self.dest_flg_1, self.dest_flg_2) = self.layout_extractor.extract_layout_data()

    def get_step_configuration(self):
        ''' get the step configuration details '''
        configuration_xml = None

        step_xml = self.gau_interface.getResourceByURI(self.step_uri)
        step_dom = parseString(step_xml)
        elems = step_dom.getElementsByTagName("configuration")

        if elems:
            configuration_xml = elems[0].toxml()

        return configuration_xml

    def cache_artifact(self, lims_id):
        ''' save artifact into a list '''
        if lims_id not in self.artifact_list:
            self.artifact_list.append(lims_id)

    def prepare_cache(self):
        ''' setup the link in cache '''
        link_xml = '<ri:links xmlns:ri="http://genologics.com/ri">'

        for lims_id in self.artifact_list:
            link = '<link uri="' + self.base_uri + 'artifacts/' + lims_id + '" rel="artifacts"/>'
            link_xml += link
        link_xml += '</ri:links>'

        try:
            link_uri = self.base_uri + "artifacts/batch/retrieve"
            artifact_xml = self.gau_interface.getBatchResourceByURI(link_uri, link_xml)
            self.artifacts_dom = parseString(artifact_xml)
        except Exception as err:
            self.logger.error(str(err))
            raise ProcessingError("Error error getting artifcats")

        self.logger.debug("Exiting prepare_cache ...")
        self.logger.debug(self.artifacts_dom.toprettyxml())

    def get_artifact(self, lims_id):
        ''' given a lims id find its artifact info and return it '''
        artifact = None

        elems = self.artifacts_dom.getElementsByTagName("art:artifact")
        for artifact_elem in elems:
            container_lims_id = artifact_elem.getAttribute("limsid")
            if container_lims_id == lims_id:
                artifact = artifact_elem
        if not artifact:
            raise Exception("Cannot find artifact in artifact cache")

        return artifact

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

    def create_container(self, container_type, container_type_name):
        ''' create a container given a container type and name '''
        container_xml = '<?xml version="1.0" encoding="UTF-8"?>'
        container_xml += ('<con:container xmlns:con="http://genologics.com/ri/container"'
                          + ' xmlns:udf="http://genologics.com/ri/userdefined">')
        container_xml += '<name>' + container_type_name + '</name>'
        container_xml += '<udf:field type="Text" name="Freezer box">31A</udf:field>'
        container_xml += '<udf:field type="Text" name="Box location">1</udf:field>'
        container_xml += ('<type uri="' + self.base_uri + 'containertypes/'
                          + container_type + '" name="' + container_type_name + '"/>')
        container_xml += '</con:container>'

        container_dom = parseString(container_xml)
        container_uri = self.base_uri + "containers"

        self.logger.debug(container_dom.toprettyxml())

        try:
            rsp_xml = self.gau_interface.createObject(container_xml, container_uri)
        except Exception as err:
            err_msg = "Cannot create a container - {}".format(str(err))
            self.logger.debug(err_msg)
            raise ProcessingError(err_msg)

        rsp_dom = parseString(rsp_xml)
        self.logger.debug(rsp_dom.toprettyxml())
        container_lims_id = None
        elems = rsp_dom.getElementsByTagName("con:container")
        if elems:
            container_lims_id = elems[0].getAttribute("limsid")

        if not container_lims_id:
            raise Exception("Cannot find LIMS id of container")

        self.logger.debug("container_lims_id: " + container_lims_id)
        return container_lims_id

    def build_source_to_destination_map(self):
        ''' build source to destination lims ids map '''
        process_uri = self.base_uri + "processes/" + self.lims_id
        process_xml = self.gau_interface.getResourceByURI(process_uri)
        process_dom = parseString(process_xml)
        self.logger.debug(process_dom.toprettyxml())

        src_dest_maps = process_dom.getElementsByTagName("input-output-map")

        for src_dest_map in src_dest_maps:
            destination = src_dest_map.getElementsByTagName("output")
            dest_type = destination[0].getAttribute("output-type")
            dest_generation_type = destination[0].getAttribute("output-generation-type")

            if dest_type == "ResultFile" and dest_generation_type == "PerInput":
                dest_lims_id = destination[0].getAttribute("limsid")
                self.cache_artifact(dest_lims_id)

                nodes = src_dest_map.getElementsByTagName("input")
                src_lims_id = nodes[0].getAttribute("limsid")
                self.cache_artifact(src_lims_id)

                ## create a map entry
                if not src_lims_id in self.src_2_dest_map:
                    self.src_2_dest_map[src_lims_id] = []
                    self.logger.debug("src_lims_id: {} dest_lims_id: {}"
                                      .format(src_lims_id, dest_lims_id))
                    self.src_2_dest_map[src_lims_id].append(dest_lims_id)

        if not self.src_2_dest_map:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        for key, value in self.src_2_dest_map.items():
            self.logger.debug("key: {} value: {}\n".format(key, value))

    def auto_place(self):
        ''' performs actual automatic placement of samples in 2 containers '''

        self.build_source_to_destination_map()

        ## build the  cache of Analytes
        self.prepare_cache()

        dest_con_lims_id = self.create_container("57", "PoolNormalisation")

        # get the configuration
        step_configuartion = self.get_step_configuration()

        # carry ou the placements of the artifacts in the container(s)
        placement_xml = '<?xml version="1.0" encoding="UTF-8"?>'
        placement_xml += ('<stp:placements xmlns:stp="http://genologics.com/ri/step" uri="'
                          + self.step_uri +  '/placements">')
        placement_xml += '<step uri="' + self.step_uri + '"/>'
        placement_xml += step_configuartion
        placement_xml += '<selected-containers>'
        placement_xml += ('<container uri="' + self.base_uri + 'containers/'
                          + dest_con_lims_id + '"/>')
        placement_xml += '</selected-containers><output-placements>'

        ## let's process our cache, one input at a time
        placement_list = []
        for sourcelims_id in self.src_2_dest_map:
            ## get the well position for the input
            src_dom = self.get_artifact(sourcelims_id)
            nodes = src_dom.getElementsByTagName("value")
            src_well = self.gau_interface.getInnerXml(nodes[0].toxml(), "value")
            ## well placement should always contain a :
            if ":" not in src_well:
                err_msg = ("Unable to determine well placement for artifact ({})"
                           .format(sourcelims_id))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            self.logger.debug("sourcelims_id: {}, src_well_placement: {}"
                              .format(sourcelims_id, src_well))
            dest_lims_ids = self.src_2_dest_map[sourcelims_id]
            for destination in dest_lims_ids:
                dest_dom = self.get_artifact(destination)
                dest_uri = dest_dom.getAttribute("uri")

                # carry out the well placements using the layout mapping data
                container_name = self.get_container_name(src_dom)
                dest_well = None
                if "1_3" in container_name:
                    dest_well = self.layout_extractor.playout_type_glmx_1_3[src_well]
                if "1_10" in container_name:
                    dest_well = self.layout_extractor.playout_type_glmx_1_10[src_well]
                else:
                    err_msg = ("Cannot find container name for: ({})"
                               .format(src_well))
                    self.logger.error(err_msg)
                    raise ProcessingError(err_msg)

                self.logger.debug("src_well_placement: {}, dest_well_placement: {}\n"
                                  .format(src_well, dest_well))
                if dest_well not in placement_list:
                    pl_xml = '<output-placement uri="' + dest_uri + '">'
                    pl_xml += ('<location><container uri="' + self.base_uri
                               + 'containers/' + dest_con_lims_id + '" limsid="'
                               + dest_con_lims_id + '"/>')
                    pl_xml += ('<value>' + dest_well
                               + '</value></location></output-placement>')
                    placement_xml += pl_xml

                    placement_list.append(dest_well)

        placement_xml += '</output-placements></stp:placements>'

        self.logger.debug(placement_xml)

        placement_dom = parseString(placement_xml)
        self.logger.debug(placement_dom.toprettyxml())

        # do the acctual placement
        placement_uri = self.step_uri + "/placements"

        try:
            rsp_xml = self.gau_interface.createObject(placement_xml, placement_uri)
        except:
            msg = "Cannot perform placement of analytes"
            self.logger.error(msg)
            raise ProcessingError(msg)
        self.extract_error_message(rsp_xml)

    def extract_error_message(self, rsp_xml):
        ''' extract error message if any '''
        rsp_dom = parseString(rsp_xml)
        self.logger.debug("placement DOM: \n{}".format(rsp_dom.toprettyxml()))
        msg = get_error_message(rsp_xml)
        if msg:
            self.logger.error(msg)
            raise ProcessingError(msg)

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automationi and do the auto placement '''

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-l", "--layoutType", help="the layout type (GLMXL, BANLRL)")
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

    exit_status = None
    exit_msg = None
    program_status = None
    try:
        multi_spl_placer_mgr = MultiSampleAutoPlacerMgr(logger, gau_interface,
                                                        args.stepURI, args.layoutType)
        multi_spl_placer_mgr.extract_layout_data(logger, args.layoutType, args.groupId)
        multi_spl_placer_mgr.auto_place()
    except ProcessingError as perr:
        exit_status = "ERROR"
        exit_msg = "Auto-placement of replicates failed - {}".format(str(perr))
        program_status = 1
        logger.error(exit_msg)
    except Exception as err:
        exit_status = "ERROR"
        exit_msg = "Auto-placement of replicates  failed - {}".format(str(err))
        program_status = 1
        logger.error(exit_msg)
    else:
        exit_status = "OK"
        exit_msg = "Auto-placement of replicates occurred successfully"
        program_status = 0
        logger.info(exit_msg)
    finally:
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)

    return program_status

if __name__ == "__main__":
    main()
