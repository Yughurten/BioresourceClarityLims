''' this script blanks the name of containers in the step '''

import os
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (log_cmdline_args,
                            get_logger,
                            set_exit_status,
                            ProcessingError
                           )

class ContainerNameValidatorMgr:
    ''' handles the updating of the analyte concentration read from data file '''
    def __init__(self, logger, gau_interface, step_uri):
        self.logger = logger
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uris = []

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
            if output_elem_type == "Analyte" and output_elem_generation_type == "PerInput":
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

    def validate_container_names(self):
        ''' update the analyte concentration and QC flag via the REST API '''
        self.logger.info("Updating Glomax concentrations ...")
        container_list = []
        for analyte_uri in self.analyte_uris:
            self.logger.debug("Analyte URI: {0}".format(analyte_uri))

            try:
                analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
                analyte_dom = parseString(analyte_xml)
                self.logger.debug(analyte_dom.toprettyxml())
                container_name = self.get_container_name(analyte_dom).strip()
                self.logger.debug("Container Name: {0}".format(container_name))

                # check thath the tube id has been entered
                if not container_name or "27-" in container_name:
                    msg = "Tube Id must be filled before proceeding"
                    raise ProcessingError(msg)

                #check for duplicate
                if container_name not in container_list:
                    container_list.append(container_name)
                else:
                    msg = "{} is a duplicate entry".format(str(container_name))
                    raise ProcessingError(msg)

                self.logger.debug("Container Name: {}".format(str(container_list)))
                rsp_xml = self.gau_interface.updateObject(analyte_dom, analyte_uri)
                rsp_dom = parseString(rsp_xml)
                self.logger.debug(rsp_dom.toprettyxml())
            except Exception as err:
                msg = "Error updating container name for analyte -  {}".format(str(err))
                raise ProcessingError(msg)


def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation and run main routine '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="username of the current user")
    parser.add_argument("-p", "--password", help="password of the current user")
    parser.add_argument("-s", "--stepURI", help="the URI of the step that triggered this script")
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
    exit_status = None
    try:
        container_name_mgr = ContainerNameValidatorMgr(logger, gau_interface, args.stepURI)
        container_name_mgr.build_list_of_analyte_uris()
        container_name_mgr.validate_container_names()
    except ProcessingError as perr:
        exit_msg = "Invalid Tube Id - {}\n".format(str(perr))
        exit_status = "ERROR"
        logger.error(exit_msg)
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        program_status = 1
    except Exception as err:
        exit_msg = "Invalid Tube Id - {}\n".format(str(err))
        exit_status = "ERROR"
        set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        logger.error(exit_msg)
        program_status = 1
    else:
        exit_status = "OK"
        exit_msg = "Tube Ids validated successfully"
        #set_exit_status(gau_interface, args.stepURI, exit_status, exit_msg)
        logger.info(exit_msg)
        program_status = 0
    finally:
        print(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
