''' this script reads the analyte molarity and
    calculates the dilution factor and the stock DNA needed '''

import os
import argparse
from xml.dom.minidom import parseString

from gls_api_util import glsapiutil
from gls_func_utils import (log_cmdline_args,
                            get_logger,
                            validate_group_id,
                            get_analyte_name,
                            SampleValueDataReader,
                            ProcessingError
                           )

class FinalPoolMgr:
    ''' handles the updating of the analyte concentration read from data file '''
    def __init__(self, logger, gau_interface, step_uri, group_id):
        self.logger = logger
        self.gau_interface = gau_interface
        tokens = step_uri.split("/")
        self.base_uri = "/".join(tokens[0:5]) + "/"
        lims_id = tokens[6]
        self.process_uri = self.base_uri + "processes/" + lims_id
        self.analyte_uris = []
        self.process_dom = None
        self.required = None
        self.volume = None
        self.pool_size = None

        status = validate_group_id(group_id)
        if not status:
            err_msg = "Invalid group id - {}".format(group_id)
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)


        self.lib_reader = SampleValueDataReader(logger, group_id, "Library.dat")
        self.lib_reader.extract_data()

    def build_list_of_analyte_uris(self):
        ''' make up a list of analytes contained in the process xml load '''
        self.logger.info("Building the artifacts list ...")
        process_xml = self.gau_interface.getResourceByURI(self.process_uri)
        self.process_dom = parseString(process_xml)
        self.logger.debug(self.process_dom.toprettyxml())

        ioutput_elements = self.process_dom.getElementsByTagName("input-output-map")
        for ioutput_elem in ioutput_elements:
            output_elem = ioutput_elem.getElementsByTagName("output")[0]
            output_elem_type = output_elem.getAttribute("output-type")
            output_elem_generation_type = output_elem.getAttribute("output-generation-type")
            if output_elem_type == "ResultFile" and output_elem_generation_type == "PerInput":
                analyte_uri = output_elem.getAttribute("uri")
                self.analyte_uris.append(analyte_uri)
                self.logger.debug("Storing analyte URI: {}\n".format(analyte_uri))

        if not self.analyte_uris:
            err_msg = "Data set empty, please check step configuration"
            self.logger.error(err_msg)
            raise ProcessingError(err_msg)

    def get_process_data(self):
        ''' extract process data from process dom '''
        self.logger.info("Extracting process data ...")
        self.required = self.gau_interface.getUDF(self.process_dom, "Required (nM)")
        if not self.required:
            err_msg = "Cannot find process Required (nM)"
            self.logger.debug(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug(("Required (nM): {}" .format(self.required)))

        self.volume = self.gau_interface.getUDF(self.process_dom, "Volume (ul)")
        if not self.required:
            err_msg = "Cannot find process Volume (ul)"
            self.logger.debug(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug(("Volume (ul): {}" .format(self.volume)))

        self.pool_size = self.gau_interface.getUDF(self.process_dom, "Pool Size")
        if not self.pool_size:
            err_msg = "Cannot find process Pool Size"
            self.logger.debug(err_msg)
            raise ProcessingError(err_msg)

        self.logger.debug(("Pool Size: {}" .format(self.pool_size)))

    def update_dilution_factor(self):
        ''' update the analyte concentration and QC flag via the REST API '''

        self.get_process_data()
        stock_dna_needed_total = 0
        water_dom = None
        water_uri = None
        for analyte_uri in self.analyte_uris:
            self.logger.debug("Analyte URI: {0}".format(analyte_uri))
            analyte_xml = self.gau_interface.getResourceByURI(analyte_uri)
            analyte_dom = parseString(analyte_xml)
            try:
                analyte_name = get_analyte_name(analyte_dom)
                if analyte_name.lower() == "water":
                    water_dom = analyte_dom
                    water_uri = analyte_uri
                    continue
                library = self.lib_reader.get_library_value(analyte_name)
                if not library:
                    err_msg = "Cannot find libarary value for " + analyte_name
                    self.logger.debug(err_msg)
                    raise ProcessingError(err_msg)
                self.logger.debug("library: {}".format(library))

                dilution_factor = float(library) / float(self.required)
                self.gau_interface.setUDF(analyte_dom, "Dilution Factor",
                                          dilution_factor,
                                          "Numeric")

                stock_dna_needed = float(self.volume)/(dilution_factor * float(self.pool_size))
                self.gau_interface.setUDF(analyte_dom, "Stock DNA needed (ul)",
                                          stock_dna_needed,
                                          "Numeric")

                stock_dna_needed_total += stock_dna_needed
                self.logger.debug(analyte_dom.toprettyxml())

                self.gau_interface.updateObject(analyte_dom, analyte_uri)
            except Exception as err:
                msg = "Error updating Dilution Factor for analyte -  {}".format(str(err))
                raise ProcessingError(msg)
        try:
            stock_dna_needed_total = float(self.volume) - stock_dna_needed_total
            self.gau_interface.setUDF(water_dom, "Stock DNA needed (ul)",
                                      stock_dna_needed_total,
                                      "Numeric")

            self.logger.debug(water_dom.toprettyxml())

            self.gau_interface.updateObject(water_dom, water_uri)
        except Exception as err:
            msg = "Error updating Stock DNA needed for Water -  {}".format(str(err))
            raise ProcessingError(msg)


def main():
    ''' program main routine: extract program tokens from the cmd line
        arguments passed by Clarity LIMS automation '''
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

    # initiliase REST API interface
    gau_interface = glsapiutil()
    gau_interface.setHostname(hostname)
    gau_interface.setup(args.username, args.password)

    program_status = None
    exit_msg = None
    try:
        final_pool_loader_mgr = FinalPoolMgr(logger, gau_interface,
                                             args.stepURI, args.groupId)
        final_pool_loader_mgr.build_list_of_analyte_uris()
        final_pool_loader_mgr.update_dilution_factor()
    except ProcessingError as perr:
        exit_msg = "Final Pool failed - {}\n".format(str(perr))
        logger.error(exit_msg)
        program_status = 1
    except Exception as err:
        exit_msg = "Final Pool failed - {}\n".format(str(err))
        logger.error(exit_msg)
        program_status = 1
    else:
        exit_msg = "Final Pool completed successfully"
        logger.info(exit_msg)
        program_status = 0
    finally:
        print(exit_msg)

    return program_status

if __name__ == "__main__":
    main()
