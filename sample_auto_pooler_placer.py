''' sample pooling script '''

import os
import csv
import argparse
from xml.dom.minidom import parseString

from default_layout_info import DEFAULT_LAYOUT_INFO
from gls_api_util import glsapiutil
from gls_func_utils import (set_exit_status,
                            validate_layout_type,
                            validate_group_id,
                            get_layout_filename,
                            get_error_message,
                            log_cmdline_args,
                            get_logger,
                            validate_well_location,
                            ProcessingError)

class PlateLayoutReader:
    ''' read the palte layout from the csv data file '''
    def __init__(self, logger, layout_type, group_id):
        self.logger = logger
        self.playout_mapping = {}
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

    def get_header_indices(self, field_names):
        ''' get the indices of the header fields '''
        src_well_idx = None
        dest_well_idx = None
        pooling_grp_idx = None
        #self.logger.debug(field_names)

        for idx, field in enumerate(field_names):
            lfield = field.strip().lower()
            if lfield == "source well":
                src_well_idx = idx
            elif lfield == "destination well":
                dest_well_idx = idx
            elif lfield == "pool group":
                pooling_grp_idx = idx

        if src_well_idx != 0 and src_well_idx is None:
            err_msg = "Expected 'Source Well' field missing in the layout file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if dest_well_idx != 0 and dest_well_idx is None:
            err_msg = "Expected 'Destination Well' field missing in the layout file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        if pooling_grp_idx != 0 and pooling_grp_idx is None:
            err_msg = "Expected 'Pool Group' field missing in the layout file"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return (src_well_idx, dest_well_idx, pooling_grp_idx)

    def extract_data(self):
        ''' extract plate layout data from csv file '''
        self.logger.debug("---->  Reading plate layout ...")
        layout_data = []
        with open(self.layout_filename, 'r') as file_descriptor:
            layout_data = list(csv.reader(file_descriptor))

        field_names = list(layout_data[0])

        (src_well_idx, dest_well_idx, pooling_grp_idx) = self.get_header_indices(field_names)
        self.logger.debug("Indices: 1: {} 2: {} 3: {}".
                          format(src_well_idx, dest_well_idx, pooling_grp_idx))

        for data in layout_data[1:]:
            try:
                src_plate = data[src_well_idx]
                validate_well_location("96", src_plate)
                src_plate_location = "{}:{}".format(src_plate[0], src_plate[1:])
                #self.logger.debug(src_plate_location)
                dest_plate = data[dest_well_idx]
                if dest_plate.lower().strip() != "tube":
                    validate_well_location("96", dest_plate)
                    dest_plate_location = "{}:{}".format(dest_plate[0], dest_plate[1:])
                else:
                    dest_plate_location = dest_plate
                pooling_grp = data[pooling_grp_idx]
                if src_plate_location not in self.playout_mapping:
                    self.playout_mapping[src_plate_location] = []
                self.playout_mapping[src_plate_location.strip()].append(dest_plate_location.strip())
                self.playout_mapping[src_plate_location.strip()].append(pooling_grp.strip())
            except Exception as err:
                err_msg = "Error extracting data from layout file - " + str(err)
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

        for key, val in self.playout_mapping.items():
            self.logger.debug("src: {}, dest: {}, grp: {}".format(key, val[0], val[1]))

        self.logger.debug("Finished extracting data\n")

class SampleAutoPoolerMgr:
    ''' handles the pooling of samples '''
    def __init__(self, logger, gau_interface, step_uri, layout_type, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        self.step_uri = step_uri
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        self.lims_id = tokens[6]
        self.artifacts_dom = []
        self.artifact_list = []
        self.pooling_grp = []

        self.playout_reader = PlateLayoutReader(logger, layout_type, group_id)
        self.playout_reader.extract_data()

    def get_step_configuration(self):
        ''' get the step configuration XML paylod '''
        configuration_xml = None

        step_xml = self.gau_interface.getResourceByURI(self.step_uri)
        step_dom = parseString(step_xml)
        elems = step_dom.getElementsByTagName("configuration")

        if elems:
            configuration_xml = elems[0].toxml()

        self.logger.debug("Step configuration: {}".format(configuration_xml))

        return configuration_xml

    def cache_artifact(self, lims_id):
        ''' store the lims id in a list for later use '''
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
        ''' given a lims id return its artifact info '''
        artifact = None
        elems = self.artifacts_dom.getElementsByTagName("art:artifact")
        for artifact_elem in elems:
            container_lims_id = artifact_elem.getAttribute("limsid")
            if container_lims_id == lims_id:
                artifact = artifact_elem
                break

        if not artifact:
            err_msg = "Cannot find artifact ({}) in artifact cache".format(lims_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return artifact

    def get_pooling_group(self, well_location): #, sample_uri):
        ''' get the pooling group for this sample '''
        #well_row = well_location[0]
        #well_columns = well_location[2:]
        #layout_location = well_row + well_columns
        try:
            pooling_grp_num = self.playout_reader.playout_mapping[well_location][1]
        except Exception as err:
            err_msg = "Cannot find pooling group for well location - " + str(err)
            self.logger.debug(err_msg)
            raise ProcessingError(err_msg)

        if int(pooling_grp_num) < 10:
            pooling_group = "Hyb0" + pooling_grp_num.strip()
        else:
            pooling_group = "Hyb" + pooling_grp_num.strip()

        return pooling_group

    #@staticmethod
    def build_group_pool_xml(self, pool_name, alist):
        ''' build the XML for group pool '''
        self.logger.debug("build_group_pool_xml called...")
        pool_xml = '<pool name="' + pool_name +'">'
        for artifact_uri in alist:
            pool_xml = pool_xml + '<input uri="' + artifact_uri + '"/>'
        pool_xml = pool_xml + '</pool>'

        return pool_xml

    def auto_pool(self):
        ''' perform the auto pooling of samples '''
        self.logger.debug("autoPool() called\n")
        pool_groups = {}

    	## step one: get the process XML
        process_uri = self.base_uri + "processes/" + self.lims_id
        self.logger.debug(process_uri)
        process_xml = self.gau_interface.getResourceByURI(process_uri)
        process_dom = parseString(process_xml)
        #self.logger.debug(process_dom.toprettyxml())

        src_maps = process_dom.getElementsByTagName("input-output-map")
        for src_map in src_maps:
            source = src_map.getElementsByTagName("input")
            src_lims_id = source[0].getAttribute("limsid")
            self.logger.debug(src_lims_id)
            self.cache_artifact(src_lims_id)

        ## build our cache of Analytes
        self.prepare_cache()

        for lims_id in self.artifact_list:
            artifact = self.get_artifact(lims_id)
            artifact_uri = artifact.getAttribute("uri")
            artifact_uri = self.gau_interface.removeState(artifact_uri)
            artifact_dom = self.get_artifact(lims_id)
            #self.logger.debug(artifact_dom.toprettyxml())
            elems = artifact_dom.getElementsByTagName("value")
            src_well_placement = self.gau_interface.getInnerXml(elems[0].toxml(), "value")
            ## well placement should always contain a :
            if ":" not in src_well_placement:
                err_msg = ("Unable to determine well placement for artifact {}"
                           .format(lims_id))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            self.logger.debug("_lims_id: {}, src_well_placement: {}"
                              .format(lims_id, src_well_placement))

            pooling_group = self.get_pooling_group(src_well_placement)
            if pooling_group not in pool_groups:
                pool_groups[pooling_group] = []
            pool_groups[pooling_group].append(artifact_uri)

        ## build the pooling XML based upon the groups
        pool_xml = '<?xml version="1.0" encoding="UTF-8"?>'
        pool_xml += ('<stp:pools xmlns:stp="http://genologics.com/ri/step" uri="'
                     + self.step_uri +  '/pools">')
        pool_xml += '<step uri="' + self.step_uri + '"/>'
        pool_xml += self.get_step_configuration()
        pool_xml += '<pooled-inputs>'

        self.logger.debug(len(pool_groups))
        for pooling_name, group_uri in sorted(pool_groups.items()):
            msg = "pooling_name: {}. group_uri: {}".format(pooling_name, group_uri)
            self.logger.debug(msg)
            pool_xml += self.build_group_pool_xml(pooling_name, group_uri)

        pool_xml += '</pooled-inputs>'
        pool_xml += '<available-inputs/>'
        pool_xml += '</stp:pools>'

        pool_dom = parseString(pool_xml)
        self.logger.debug(pool_dom.toprettyxml())

        try:
            rsp_xml = self.gau_interface.updateObject(pool_dom, self.step_uri + "/pools")
        except Exception as err:
            err_msg = "Auto pooling failed: " + str(err)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug(rsp_xml)

        msg = get_error_message(rsp_xml)
        if msg:
            self.logger.error(msg)
            raise ProcessingError(msg)

class SampleAutoPlacerMgr:
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
        self.source_2_destination_map = {}   #a mapping of inputs to their destinations

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

        link_dom = parseString(link_xml)
        self.logger.debug("link_dom: {}".format(link_dom.toprettyxml()))

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
        self.logger.debug(container_dom.toprettyxml())

        container_uri = self.base_uri + "containers"

        try:
            container_xml = self.gau_interface.createObject(container_xml, container_uri)
        except Exception as err:
            err_msg = "Cannot create a container - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        container_dom = parseString(container_xml)
        self.logger.debug(container_dom.toprettyxml())
        container_lims_id = None
        elems = container_dom.getElementsByTagName("con:container")
        if elems:
            container_lims_id = elems[0].getAttribute("limsid")

        if not container_lims_id:
            err_msg = "Cannot find LIMS id of container"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        return container_lims_id

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

            if destination_type == "Sample":
                destination_lims_id = destination[0].getAttribute("limsid")
                self.cache_artifact(destination_lims_id)

                nodes = source_destination_map.getElementsByTagName("input")
                source_lims_id = nodes[0].getAttribute("limsid")
                self.cache_artifact(source_lims_id)

                ## create a map entry
                if not source_lims_id in self.source_2_destination_map:
                    self.source_2_destination_map[source_lims_id] = []

                self.logger.debug("source_lims_id: {} destination_lims_id: {}"
                                  .format(source_lims_id, destination_lims_id))
                self.source_2_destination_map[source_lims_id].append(destination_lims_id)

        if not self.source_2_destination_map:
            err_msg = "Data set empty, please check step configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        for key, value in self.source_2_destination_map.items():
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
        self.logger.debug("number of limsids: {}".format(len(self.source_2_destination_map)))
        for source_lims_id in self.source_2_destination_map:
            # get the well position for the input
            source_dom = self.get_artifact(source_lims_id)
            #self.logger.debug(source_dom.toprettyxml())
            elems = source_dom.getElementsByTagName("value")
            src_well_placement = self.gau_interface.getInnerXml(elems[0].toxml(), "value")
            ## well placement should always contain a :
            if ":" not in src_well_placement:
                err_msg = ("Unable to determine well placement for artifact {}"
                           .format(source_lims_id))
                self.logger.error(err_msg)
                raise ProcessingError(err_msg)

            self.logger.debug("source_lims_id: {}, src_well_placement: {}"
                              .format(source_lims_id, src_well_placement))
            destination_lims_ids = self.source_2_destination_map[source_lims_id]
            self.logger.debug("source_lims_id: {} destination_lims_ids: {}"
                              .format(source_lims_id, str(destination_lims_ids)))
            self.logger.debug("destination_lims_ids: {}".format(len(destination_lims_ids)))
            for destination in destination_lims_ids:
                destination_dom = self.get_artifact(destination)
                destination_uri = destination_dom.getAttribute("uri")

                self.logger.debug("destination_don: \n" + destination_dom.toprettyxml())

                if not self.playout_reader.playout_mapping[src_well_placement]:
                    err_msg = "Missing destination well in plate layout: {}".format(destination)
                    self.logger.error(err_msg)
                    raise ProcessingError(err_msg)

                dest_well_placement = self.playout_reader.playout_mapping[src_well_placement][0]
                self.logger.debug("src_well_placement: {}, dest_well_placement: {}"
                                  .format(src_well_placement, dest_well_placement))

                if dest_well_placement not in placement_list:
                    pl_xml = '<output-placement uri="' + destination_uri + '">'
                    pl_xml += ('<location><container uri="' + self.base_uri
                               + 'containers/' + destination_container
                               + '" limsid="' + destination_container + '"/>')
                    pl_xml += ('<value>' + dest_well_placement
                               + '</value></location></output-placement>')

                    placement_xml += pl_xml

                    placement_list.append(dest_well_placement)

        placement_xml += '</output-placements></stp:placements>'

        #placement_dom = parseString(placement_xml)
        #self.logger.debug(placement_dom.toprettyxml())

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

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
    parser.add_argument("-l", "--layoutType", help="the layout type")
    parser.add_argument("-m", "--useMode", help="the operation mode pooler or placer")
    parser.add_argument("-g", "--groupId", help="the work group that run this script")
    args = parser.parse_args()

    prog_name = os.path.basename(__file__)
    logger = get_logger(prog_name)

    tokens = args.stepURI.split("/")
    hostname = "/".join(tokens[2:3])

    log_cmdline_args(logger, prog_name, hostname, args.username, args.password, args.stepURI)

    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    program_status = 0
    try:
        exit_status = None
        exit_msg = None
        if args.useMode == "Pooler":
            auto_pooler_mgr = SampleAutoPoolerMgr(logger, gau_interface, args.stepURI,
                                                  args.layoutType, args.groupId)
            auto_pooler_mgr.auto_pool()
        elif args.useMode == "Placer":
            auto_placer_mgr = SampleAutoPlacerMgr(logger, gau_interface, args.stepURI,
                                                  args.layoutType, args.groupId)
            auto_placer_mgr.auto_place()
        else:
            err_msg = ("Incorrect us mode ({}), should be (Pooler) or (Placer)"
                       .format(args.useMode))
            raise ProcessingError(err_msg)
    except ProcessingError as perr:
        exit_status = "ERROR"
        if args.useMode == "Pooler":
            exit_msg = "Auto-pooling of replicates failed - {}".format(str(perr))
        else:
            exit_msg = "Auto-placement of replicates failed - {}".format(str(perr))
        logger.error(exit_msg)
        program_status = 1
    except Exception as err:
        exit_status = "ERROR"
        if args.useMode == "Pooler":
            exit_msg = "Auto-pooling of replicates failed - {}".format(str(err))
        else:
            exit_msg = "Auto-placement of replicates failed - {}".format(str(err))
        logger.error(exit_msg)
        program_status = 1
    else:
        exit_status = "OK"
        if args.useMode == "Pooler":
            exit_msg = "Auto-pooling of replicates completed successfully"
        else:
            exit_msg = "Auto-placement of replicates completed successfully"
        logger.info(exit_msg)
        program_status = 0
    finally:
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)

    return program_status

if __name__ == "__main__":
    main()
