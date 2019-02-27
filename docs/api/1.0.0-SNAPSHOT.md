# API Docs - v1.0.0-SNAPSHOT

## Sink

### hl7 *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word">The hl7 sink publishes the hl7 messages using MLLP protocol. </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="hl7", uri="<STRING>", hl7.encoding="<STRING>", hl7.ack.encoding="<STRING>", charset="<STRING>", tls.enabled="<BOOL>", tls.keystore.type="<STRING>", tls.keystore.filepath="<STRING>", tls.keystore.passphrase="<STRING>", hl7.timeout="<INT>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word">The URI that used to connect to a HL7 Server. <br>&nbsp;e.g.,<br><code>{hostname}:{port}</code>, <br><code>hl7://{hostname}:{port}</code> <br><code>{hostname}:{port}</code> is preferable.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.encoding</td>
        <td style="vertical-align: top; word-wrap: break-word">Encoding method of hl7. This can be er7 or xml. User should define hl7 encoding type according to the input. <br>e.g., <br>If the transmitting message is in <code>er7</code>(text) format then the encoding type should be <code>er7</code>. <br>If the transmitting message is in <code>xml</code> format then the encoding type should be <code>xml</code>. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.ack.encoding</td>
        <td style="vertical-align: top; word-wrap: break-word">Encoding method of hl7 to log the acknowledgment message. This parameter can be specified as <code>xml</code> if required. Otherwise, system uses <code>er7</code> format as default. </td>
        <td style="vertical-align: top">ER7</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">charset</td>
        <td style="vertical-align: top; word-wrap: break-word">Character encoding method. Charset can be specified if required. Otherwise, system uses <code>UTF-8</code> as default charset. </td>
        <td style="vertical-align: top">UTF-8</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter specifies whether an encrypted communication should be established or not. When this parameter is set to <code>true</code>, the <code>tls.keystore.path</code> and <code>tls.keystore.passphrase</code> parameters are initialized. </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.type</td>
        <td style="vertical-align: top; word-wrap: break-word">The type for the keystore. A custom keystore type can be specified if required. If no custom keystore type is specified, then the system uses <code>JKS</code> as the default keystore type. </td>
        <td style="vertical-align: top">JKS</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.filepath</td>
        <td style="vertical-align: top; word-wrap: break-word">The file path to the location of the keystore of the client that sends the HL7 events via the <code>MLLP</code> protocol. A custom keystore can be specified if required. If a custom keystore is not specified, then the system uses the default <code>wso2carbon</code> keystore in the <code>${carbon.home}/resources/security</code> directory. </td>
        <td style="vertical-align: top">${carbon.home}/resources/security/wso2carbon.jks</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.passphrase</td>
        <td style="vertical-align: top; word-wrap: break-word">The passphrase for the keystore. A custom passphrase can be specified if required. If no custom passphrase is specified, then the system uses <code>wso2carbon</code> as the default passphrase. </td>
        <td style="vertical-align: top">wso2carbon</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">This period of time (in milliseconds) the initiator will wait for a response for a given message before timing out and throwing an exception. </td>
        <td style="vertical-align: top">10000</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('Hl7TestAppForER7') 
@sink(type = 'hl7', 
uri = 'localhost:1080', 
hl7.encoding = 'er7', 
@map(type = 'text', @payload("{{payload}}"))) 
define stream hl7stream(payload string); 

```
<p style="word-wrap: break-word">This publishes the HL7 messages in ER7 format, receives and logs the acknowledgement message in the console using MLLP protocol and custom text mapping. <br>&nbsp;</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@App:name('Hl7TestAppForXML') 
@sink(type = 'hl7', 
uri = 'localhost:1080', 
hl7.encoding = 'xml', 
@map(type = 'xml', enclosing.element="<ADT_A01  xmlns='urn:hl7-org:v2xml'>", @payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.3><HD.1>{{MSH3HD1}}</HD.1></MSH.3><MSH.4><HD.1>{{MSH4HD1}}</HD.1></MSH.4><MSH.5><HD.1>{{MSH5HD1}}</HD.1></MSH.5><MSH.6><HD.1>{{MSH6HD1}}</HD.1></MSH.6><MSH.7>{{MSH7}}</MSH.7><MSH.8>{{MSH8}}</MSH.8><MSH.9><CM_MSG.1>{{CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{CM_MSG2}}</CM_MSG.2></MSH.9><MSH.10>{{MSH10}}</MSH.10><MSH.11>{{MSH11}}</MSH.11><MSH.12>{{MSH12}}</MSH.12></MSH>'))) 
define stream hl7stream(MSH1 string, MSH2 string, MSH3HD1 string, MSH4HD1 string, MSH5HD1 string, MSH6HD1 string, MSH7 string, MSH8 string, CM_MSG1 string, CM_MSG2 string,MSH10 string,MSH11 string, MSH12 string); 

```
<p style="word-wrap: break-word">This publishes the HL7 messages in XML format, receives and logs the acknowledgement message in the console using MLLP protocol and custom xml mapping. <br>&nbsp;</p>

## Source

### hl7 *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">The hl7 source consumes the hl7 messages using MLLP protocol. </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="hl7", port="<INT>", hl7.encoding="<STRING>", hl7.ack.encoding="<STRING>", charset="<STRING>", tls.enabled="<BOOL>", tls.keystore.filepath="<STRING>", tls.keystore.type="<STRING>", tls.keystore.passphrase="<STRING>", hl7.conformance.profile.used="<BOOL>", hl7.conformance.profile.file.path="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">port</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the unique logical address used to establish the connection for the process. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.encoding</td>
        <td style="vertical-align: top; word-wrap: break-word">Encoding method of received hl7. This can be er7 or xml. User should define hl7 encoding type according to their mapping. <br>e.g., <br>If text mapping is used, then the hl7 encoding type should be er7. <br>If xml mapping is used, then the hl7 encoding type should be xml. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.ack.encoding</td>
        <td style="vertical-align: top; word-wrap: break-word">Encoding method of hl7 to log the acknowledgment message. This parameter can be specified as xml if required. Otherwise, system uses er7 format as default. </td>
        <td style="vertical-align: top">ER7</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">charset</td>
        <td style="vertical-align: top; word-wrap: break-word">Character encoding method. Charset can be specified if required. Otherwise, system uses UTF-8 as default charset. </td>
        <td style="vertical-align: top">UTF-8</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter specifies whether an encrypted communication should be established or not. When this parameter is set to <code>true</code>, the <code>tls.keystore.path</code> and <code>tls.keystore.passphrase</code> parameters are initialized. </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.filepath</td>
        <td style="vertical-align: top; word-wrap: break-word">The file path to the location of the keystore of the client that sendsthe HL7 events via the <code>MLLP</code> protocol. A custom keystore can bespecified if required. If a custom keystore is not specified, then the systemuses the default <code>wso2carbon</code> keystore in the <code>${carbon.home}/resources/security</code> directory. </td>
        <td style="vertical-align: top">${carbon.home}/resources/security/wso2carbon.jks</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.type</td>
        <td style="vertical-align: top; word-wrap: break-word">The type for the keystore. A custom keystore type can be specified if required. If no custom keystore type is specified, then the system uses <code>JKS</code> as the default keystore type.</td>
        <td style="vertical-align: top">JKS</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.keystore.passphrase</td>
        <td style="vertical-align: top; word-wrap: break-word">The passphrase for the keystore. A custom passphrase can be specified if required. If no custom passphrase is specified, then the system uses <code>wso2carbon</code> as the default passphrase.</td>
        <td style="vertical-align: top">wso2carbon</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.conformance.profile.used</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter specifies whether a <code>conformance profile</code> is used to validate the incoming message or not. When the parameter is set to <code>true</code>, the hl7.conformance.profile.file.name should be initialized by user. If the conformance profile is used then It will send the error details along with the acknowledgment. </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">hl7.conformance.profile.file.path</td>
        <td style="vertical-align: top; word-wrap: break-word">Path conformance profile file that is used to validate the incoming message. User should give the file path, if conformance profile is used to validate the message. </td>
        <td style="vertical-align: top">Empty</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('Hl7TestAppForTextMapping') 
@source(type = 'hl7', 
port = '1080', 
hl7.encoding = 'er7', 
@map(type = 'text'))
define stream hl7stream(payload string); 

```
<p style="word-wrap: break-word">This receives the HL7 messages and sends the acknowledgement message to the client using the MLLP protocol and text mapping. <br>&nbsp;</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@App:name('Hl7TestAppForXMLMapping') 
@source(type = 'hl7', 
port = '1080', 
hl7.encoding = 'xml', 
@map(type = 'xml', namespaces = 'ns=urn:hl7-org:v2xml', @attributes(MSH10 = "ns:MSH/ns:MSH.10", MSH3HD1 = "ns:MSH/ns:MSH.3/ns:HD.1")))
define stream hl7stream (MSH10 string, MSH3HD1 string); 

```
<p style="word-wrap: break-word">This receives the HL7 messages nd send the acknowledgement message to the client using the MLLP protocol and custom xml mapping. <br>&nbsp;</p>

