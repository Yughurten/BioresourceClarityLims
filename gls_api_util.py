import urllib.request, urllib.error, urllib.parse
import re
import sys
import xml.dom.minidom

#import requests

from xml.dom.minidom import parseString
from optparse import OptionParser
from xml.sax.saxutils import escape

DEBUG = 0

class glsapiutil:
    ## Housekeeping methods

    def __init__(self):
        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))
        self.hostname = ""
        self.auth_handler = ""
        self.version = "v2"
        self.uri = ""

    def setHostname(self, hostname):
        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))
        self.hostname = hostname

    def setVersion(self, version):
        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))
        self.version = version

    def setURI(self, uri):
        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))
        self.uri = uri

    def setup(self, user, password):

        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))

        ## setup up API plumbing
        password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, "https://" + self.hostname + '/api/' + self.version, user, password)
        self.auth_handler = urllib.request.HTTPBasicAuthHandler(password_manager)
        opener = urllib.request.build_opener(self.auth_handler)
        urllib.request.install_opener(opener)
    ## REST methods


    def deleteObject(self, xmlObject, url):

        opener = urllib.request.build_opener(self.auth_handler)

        req = urllib.request.Request(url)
        #req.add_data(xmlObject)
        req.data = xmlObject.encode()
        req.get_method = lambda: 'DELETE'
        req.add_header('Accept', 'application/xml')
        req.add_header('Content-Type', 'application/xml')
        req.add_header('User-Agent', 'Python-urllib2/2.4')

        responseText = "EMPTY"

        try:
            response = opener.open(req)
            responseText = response.read()
        except urllib.error.HTTPError as e:
            responseText = e.read()
        except:
            responseText = str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1])

        return responseText

    def createObject(self, xmlObject, url):

        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))

        opener = urllib.request.build_opener(self.auth_handler)

        req = urllib.request.Request(url)
        req.data = xmlObject.encode()
        req.get_method = lambda: 'POST'
        req.add_header('Accept', 'application/xml')
        req.add_header('Content-Type', 'application/xml')
        req.add_header('User-Agent', 'Python-urllib2/2.6')

        try:
            response = opener.open(req)
            responseText = response.read()
        except urllib.error.HTTPError as e:
            responseText = e.read()
        except:
            responseText = str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1])

        return responseText

    def updateObject(self, xmlObject, url):

        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))

        opener = urllib.request.build_opener(self.auth_handler)

        req = urllib.request.Request(url)
        req.data = xmlObject.toxml().encode()
        req.get_method = lambda: 'PUT'
        req.add_header('Accept', 'application/xml')
        req.add_header('Content-Type', 'application/xml')
        req.add_header('User-Agent', 'Python-urllib/3.6.3')

        try:
            response = opener.open(req)
            responseText = response.read()
        except urllib.error.HTTPError as e:
            responseText = str(e)
        except:
            responseText = str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1])

        return responseText

    def getResourceByURI(self, url):

        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))

        responseText = ""
        xml = ""

        opener = urllib.request.build_opener(self.auth_handler)

        try:
            xml = urllib.request.urlopen(url).read()
        except urllib.error.HTTPError as e:
            responseText = e.msg
        except urllib.error.URLError as e:
            if e.strerror is not None:
                responseText = e.strerror
            elif e.reason is not None:
                responseText = str(e.reason)
        except:
            responseText = str(sys.exc_info()[0]) + str(sys.exc_info()[1])


        return xml

    def getBatchResourceByURI(self, url, links):

        if DEBUG > 0: print("{}:{} called".format(self.__module__, sys._getframe().f_code.co_name))

        opener = urllib.request.build_opener(self.auth_handler)

        req = urllib.request.Request(url)
        req.data = links.encode()
        req.get_method = lambda: 'POST'
        req.add_header('Accept', 'application/xml')
        req.add_header('Content-Type', 'application/xml')
        req.add_header('User-Agent', 'Python-urllib2/3.6')

        try:
            response = opener.open(req)
            responseText = response.read()
        except urllib.error.HTTPError as e:
            responseText = e.read()
        except:
            responseText = str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1])

        return responseText

    ## Helper methods

    @staticmethod
    def getUDF(DOM, udfname):

        response = ""

        elements = DOM.getElementsByTagName("udf:field")
        for udf in elements:
            temp = udf.getAttribute("name")
            if temp == udfname:
                response = udf.firstChild.data
                break

        return response

    @staticmethod
    def setUDF(DOM, udfname, udfvalue, udftype=None):

        if DEBUG > 2: print(DOM.toprettyxml())

        ## are we dealing with batch,
        if DOM.parentNode is None:
            isBatch = False
        else:
            isBatch = True

        newDOM = xml.dom.minidom.getDOMImplementation()
        newDoc = newDOM.createDocument(None, None, None)

        ## if the node already exists, delete it
        elements = DOM.getElementsByTagName("udf:field")
        for element in elements:
            if element.getAttribute("name") == udfname:
                try:
                    if isBatch:
                        DOM.removeChild(element)
                    else:
                        DOM.childNodes[0].removeChild(element)
                except xml.dom.NotFoundErr as e:
                    if DEBUG > 0: print("Unable to Remove existing UDF node")
                    print("Unable to Remove existing UDF node: {}".format(element.getAttribute("name")))

        # now add the new UDF node
        txt = newDoc.createTextNode(str(udfvalue))

        newNode = newDoc.createElement("udf:field")
        if udftype:
            newNode.setAttribute("type", udftype)
        newNode.setAttribute("name", udfname)
        newNode.appendChild(txt)

        if isBatch:
            DOM.appendChild(newNode)

        else:
            DOM.childNodes[0].appendChild(newNode)

        return DOM

    def getParentProcessURIs(self, pURI):

        response = []

        pXML = self.getResourceByURI(pURI)
        pDOM = parseString(pXML)
        elements = pDOM.getElementsByTagName("input")
        for element in elements:
            ppNode = element.getElementsByTagName("parent-process")
            ppURI = ppNode[0].getAttribute("uri")

            if ppURI not in response:
                response.append(ppURI)

        return response

    def getDaughterProcessURIs(self, pURI):

        response = []
        outputs = []

        pXML = self.getResourceByURI(pURI)
        pDOM = parseString(pXML)
        elements = pDOM.getElementsByTagName("output")
        for element in elements:
            limsid = element.getAttribute("limsid")
            if limsid not in outputs:
                outputs.append(limsid)

        ## now get the processes run on each output limsid
        for limsid in outputs:
            uri = self.hostname + "/api/" + self.version + "/processes?inputartifactlimsid=" + limsid
            pXML = self.getResourceByURI(uri)
            pDOM = parseString(pXML)
            elements = pDOM.getElementsByTagName("process")
            for element in elements:
                dURI = element.getAttribute("uri")
                if dURI not in response:
                    response.append(dURI)

        return response

    def reportScriptStatus(self, uri, status, message):

        newuri = uri + "/programstatus"
        message = escape(message)

        thisXML = self.getResourceByURI(newuri)
        thisDOM = parseString(thisXML)

        sNodes = thisDOM.getElementsByTagName("status")
        if len(sNodes) > 0:
            sNodes[0].firstChild.data = status
        mNodes = thisDOM.getElementsByTagName("message")
        if mNodes:
            mNodes[0].firstChild.data = message
        else: 
            newDOM = xml.dom.minidom.getDOMImplementation()
            newDoc = newDOM.createDocument(None, None, None)

            # now add the new message node
            txt = newDoc.createTextNode(str(message))
            newNode = newDoc.createElement("message")
            newNode.appendChild(txt)

            thisDOM.childNodes[0].appendChild(newNode)

        try:
            self.updateObject(thisDOM.toxml(), newuri)
        except:
            print(message)

    @staticmethod
    def removeState(xml):

        return re.sub("(.*)(\?state=[0-9]*)(.*)", "\\1" + "\\3", xml)

    @staticmethod
    def getInnerXml(xml, tag):
        tagname = '<' + tag + '.*?>'
        inXml = re.sub(tagname, '', xml)

        tagname = '</' + tag + '>'
        inXml = inXml.replace(tagname, '')

        return inXml
