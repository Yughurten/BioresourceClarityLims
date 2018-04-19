''' places samples from Glomax or BioAnalyser plate onto
   2 separate containers using a well placement layout file '''

import os
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (get_logger,
                            log_cmdline_args,
                            validate_group_id,
                            get_analyte_name,
                            ProcessingError
                           )

class AgilentSizesWriter:
    ''' performs automated placement of samples '''
    def __init__(self, logger, gau_interface, step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        #self.step_uri = step_uri
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uri_list = []
        self.file_descriptor = None

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

        filename = ("/opt/gls/clarity/data/{}/Concentrations/AgilentSizes.dat"
                    .format(group_id))
        try:
            self.file_descriptor = open(filename, 'w')
        except IOError as err:
            err_msg = "Error opening AgigentSizes data file - {}".format(str(err))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)
        except Exception as err:
            err_msg = ("Error opening AgigentSizes data file - {}"
                       .format(str(err)))
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

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
                output_elem_generation_type = output_elem.getAttribute("output-generation-type")
                if ((output_elem_type == "Analyte" or output_elem_type == "ResultFile") and
                        output_elem_generation_type == "PerInput"):
                    analyte_uri = output_elem.getAttribute("uri")
                    self.analyte_uri_list.append(analyte_uri)
                    self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uri_list:
            err_msg = "Data set empty, please check configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def save_agilent_size_values(self):
        ''' get the agilent size data from LIMS and write it to a file '''
        self.build_analyte_uri_list()
        file_header = "Sample Name,Value\n"
        self.file_descriptor.write(file_header)
        for analyte_uri in self.analyte_uri_list:
            self.logger.debug("Analyte URI: {}".format(analyte_uri))

            try:
                analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
                analyte_dom = parseString(analyte_xml)

                #sample_name = get_sample_name(self.gau_interface, analyte_dom)
                analyte_name = get_analyte_name(analyte_dom)
                self.logger.debug(analyte_dom.toprettyxml())

                agilent_size = self.gau_interface.getUDF(analyte_dom, "Agilent Size")
                self.logger.debug(("analyte_name: {} agilent_size: {}"
                                   .format(analyte_name, agilent_size)))
                if not agilent_size:
                    err_msg = "Cannot find Agilent Size value for " + analyte_name
                    self.logger.debug(err_msg)
                    raise ProcessingError(err_msg)
                data = "{},{}\n".format(analyte_name, agilent_size)
                self.file_descriptor.write(data)
            except Exception:
                err_msg = "Error saving Agilent Size value"
                self.logger.debug(err_msg)
                raise ProcessingError(err_msg)

def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automationi and run the main routine '''

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
        agilent_sizes_writer = AgilentSizesWriter(logger, gau_interface,
                                                  args.stepURI, args.groupId)
        agilent_sizes_writer.save_agilent_size_values()
    except ProcessingError as perr:
        exit_msg = "Write of Agilent Sizes failed - {}".format(str(perr))
        program_status = 1
    except Exception as err:
        exit_msg = "Write of Agilent Sizes failed - {}".format(str(err))
        program_status = 1
    else:
        exit_msg = "Write of Agilent Sizes occurred successfully"
        program_status = 0
    finally:
        print(exit_msg)
        logger.debug(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
