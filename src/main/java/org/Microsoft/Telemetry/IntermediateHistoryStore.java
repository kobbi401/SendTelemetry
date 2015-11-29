/*
 * ApplicationInsights-Java
 * Copyright (c) Microsoft Corporation
 * All rights reserved.
 *
 * MIT License
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the ""Software""), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package org.Microsoft.Telemetry;

import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.TelemetryConfiguration;
import com.microsoft.applicationinsights.telemetry.EventTelemetry;
import com.microsoft.applicationinsights.telemetry.SeverityLevel;
import com.microsoft.applicationinsights.telemetry.TraceTelemetry;
import org.apache.hadoop.service.AbstractService;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import java.lang.reflect.*;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * this Class to send History intermediate of Job to Application Insights , Add
 * also preserved in history in class of History storage of Yarn Timeline server
 *
 * @author b-yaif
 */
public class IntermediateHistoryStore extends AbstractService
        implements TimelineStore {

    private static final Log LOG = LogFactory.getLog(IntermediateHistoryStore.class);

    private String Ikey = "";
    private String job_id = "";
    private String IKEY_NAME_PROPERTY_CONFIG = "microsoft.telemetry.IKey";
    private String PREFIX_CUSTOM_DIMENSINS = "microsoft.telemetry.custom.parameter.";
    private String PATTERN_LOG_INFO = " [ Telemetry LI ] ";
    private String PATTERN_LOG_ERROR = " [ Telemetry LE ] ";

    private TelemetryClient telemetry = null;
    private TelemetryConfiguration telemetryconfig = null;
    private TimelineStore originalStorage = null;
    private YarnClient client = null;
    private Configuration config = new YarnConfiguration();
    private ApplicationReport applicationReport = null;

    private Map<String, Long> times = new HashMap<String, Long>();
    private Map<String, String> dimension_to_sending = new HashMap<String, String>();
    private Map<String, String> dimension_from_config = new HashMap<String, String>();

    //Constructor overload
    public IntermediateHistoryStore(String name) throws YarnException, IOException {
        super(IntermediateHistoryStore.class.getName());

        try {
            initialization(config);
        } catch (IOException ex) {
            Logger.getLogger(IntermediateHistoryStore.class.getName()).log(Level.SEVERE, null, ex);
        } catch (YarnException ye) {
            Logger.getLogger(IntermediateHistoryStore.class.getName()).log(Level.SEVERE, null, ye);
        }

    }

    //Default constructor
    public IntermediateHistoryStore() throws YarnException, IOException {
        super(IntermediateHistoryStore.class.getName());

        try {
            initialization(config);
        } catch (IOException ex) {
            Logger.getLogger(IntermediateHistoryStore.class.getName()).log(Level.SEVERE, null, ex);
        } catch (YarnException ye) {
            Logger.getLogger(IntermediateHistoryStore.class.getName()).log(Level.SEVERE, null, ye);
        }
    }

    /**
     * Initialize the service.
     *
     * The transition MUST be from {@link STATE#NOTINITED} to
     * {@link STATE#INITED} unless the operation failed and an exception was
     * raised, in which case {@link #stop()} MUST be invoked and the service
     * enter the state {@link STATE#STOPPED}.
     *
     * @param conf the configuration of the service
     */
    @Override
    public void init(Configuration conf) {
        this.config = conf;

        if (originalStorage != null) {
            this.originalStorage.init(conf);
        } else {
            LOG.info(PATTERN_LOG_INFO + "origina Storage  not initialized");
        }

        if (client != null) {
            this.client.init(conf);
        } else {
            LOG.info(PATTERN_LOG_INFO + " Yarn Client not initialized");
        }

    }

    /**
     * This method Initializes all objects of the Telemetry service.
     *
     * @throws IOException ,YarnException
     */
    private void initialization(Configuration conf) throws YarnException, IOException {

        originalStorage = ReflectionUtils.newInstance(conf.getClass(
                YarnConfiguration.TIMELINE_SERVICE_STORE + "-old", LeveldbTimelineStore.class,
                TimelineStore.class), conf);

        telemetryconfig = TelemetryConfiguration.getActive();

        client = YarnClient.createYarnClient();

        Ikey = conf.get(IKEY_NAME_PROPERTY_CONFIG);

        dimension_from_config = conf.getValByRegex(PREFIX_CUSTOM_DIMENSINS + "*");

        LOG.info(PATTERN_LOG_INFO + String.format("Updating %d dimensions from Configuration file ", dimension_from_config.size()));

        if (!Ikey.equals("")) {

            telemetryconfig.setInstrumentationKey(Ikey);
            telemetry = new TelemetryClient(telemetryconfig);
            LOG.info(PATTERN_LOG_INFO + "Instrumentation Key  initialized successfully....!");

        } else {
            LOG.error(PATTERN_LOG_ERROR + "Instrumentation Key is not initialized Because not provided Ikey or failing to read from config file ");
        }

        if (originalStorage != null) {

            LOG.info(PATTERN_LOG_INFO + "Timeline server Storage initialized successfully....!");

        } else {
            LOG.error(PATTERN_LOG_ERROR + "Timeline server Storage Initialization failed");
        }
    }

    /**
     * This method convert http response to Document.
     *
     * @param inputStream all response from the server
     * @return An {@link Document} object describe all information of respond in
     * XML.
     * @throws Exception
     */
    private Document convertInputStreamToDocumen(InputStream inputStream) {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            Document doc = builder.parse(inputStream);
            return doc;
        } catch (Exception e) {
            LOG.error(PATTERN_LOG_ERROR + "Error at convert http response to Documant ", e);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This method returns all request dimensions values as pairs .
     *
     * @param dimension , all dimension from Configuration file
     * @param RootDocument , Document all Configuration file as xml
     * @return An {@link Map} , dimensions values as pair{ name , value } .
     * @throws XPathExpressionException
     */
    private Map<String, String> get_dimensions_values(Map<String, String> dimension, Document RootDocument) throws XPathExpressionException {

        String expression = "";
        Map<String, String> values_of_dimensions = new HashMap<String, String>();

        if (RootDocument != null) {

            NodeList propertyNodes = RootDocument.getElementsByTagName("property");
            XPath xPath = XPathFactory.newInstance().newXPath();

            for (Map.Entry<String, String> entrySet : dimension.entrySet()) {

                String value = entrySet.getValue();

                expression = String.format("/conf/property[name = '%s']/value", value);

                NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(RootDocument, XPathConstants.NODESET);

                if (nodeList.getLength() >= 1) {
                    values_of_dimensions.put(value, nodeList.item(0).getFirstChild().getNodeValue());
                } else {
                    values_of_dimensions.put(value, " not found value ");
                }

            }
        } else {
            LOG.error(PATTERN_LOG_ERROR + "Root Document is null ");
        }

        return values_of_dimensions;
    }

    /**
     * This method send http request by MAPREDUCE RESY API to get Configuration
     * file from ResourceManager To obtain the values of properties .
     *
     * @param dimension , all dimension from Configuration file
     * @param Appid , Application id
     * @return An {@link Map} , dimensions values as
     * pair{dimension_name,dimension_value}.
     * @throws YarnException, IOException
     */
    private Map<String, String> get_properties_values(Map<String, String> dimension, String Appid) throws YarnException, IOException {

        String TrackingUrl = "";
        String ConfUrl = "empty";
        Map<String, String> dimension_value = new HashMap<String, String>();
        Appid = Appid.replaceFirst("Application", "application");
        String Jobid = Appid.replaceFirst("application", "job");
        HttpURLConnection connection = null;

        try {

            if (client != null) {

                ApplicationId app = ConverterUtils.toApplicationId(Appid);

                if (app != null) {

                    applicationReport = client.getApplicationReport(app);
                    LOG.info(PATTERN_LOG_INFO + "Create ApplicationId succeeded");
                } else {
                    LOG.error(PATTERN_LOG_ERROR + "Create ApplicationId Failed");
                }

            } else {
                LOG.error(PATTERN_LOG_ERROR + "Create Yarnclient Failed");
            }

            if (applicationReport != null) {

                LOG.info(PATTERN_LOG_INFO + "Create applicationReport succeeded");
                TrackingUrl = applicationReport.getTrackingUrl();
                ConfUrl = String.format("%sws/v1/mapreduce/jobs/%s/conf", TrackingUrl, Jobid);

            } else {
                LOG.error(PATTERN_LOG_ERROR + "Create applicationReport Failed");
            }

            if (!ConfUrl.equals("empty")) {

                LOG.info(PATTERN_LOG_INFO + "Connecting to ResourceManager at address  " + ConfUrl);

                URL url = new URL(ConfUrl);
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestProperty("Accept", "application/xml");

                InputStream is = null;
                int statusCode = connection.getResponseCode();
                LOG.info(PATTERN_LOG_INFO + String.format("statusCode :%d", statusCode));
                if (statusCode >= 200 && statusCode < 400) {

                    //Get Response 
                    is = connection.getInputStream();
                    Document XmlConf = convertInputStreamToDocumen(is);
                    dimension_value = get_dimensions_values(dimension, XmlConf);

                } else {
                    is = connection.getErrorStream();
                    LOG.error(PATTERN_LOG_ERROR + String.format("Error : http request failed  statusCode is %d", statusCode));
                }
            } else {
                LOG.error(PATTERN_LOG_ERROR + "Connects to ResourceManager failed Because http Incorrect address ......");
            }

        } catch (Exception e) {

            LOG.error(PATTERN_LOG_ERROR + "Error : ", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }

        }

        return dimension_value;

    }

    private void WriteToFileLog(String key, String value) throws Exception {
        try {

            LOG.info(PATTERN_LOG_INFO + key + "  " + value + "\n");

        } catch (Exception e) {

            String message = PATTERN_LOG_ERROR + " Creating a problem while writing to Log";
            LOG.error(message, e);
            throw e;
        }
    }

    private String CastFromTimeToStringFormat(long Timestamp) {

        Date date = new Date((long) Timestamp);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);

    }

    /**
     * This method Check if the job he MapReduce type
     *
     * @param EntityType ,Entity Type as {@link String}
     * @return An {@link boolean}
     */
    private boolean if_MapReducejob(String EntityType) {

        String[] Entity_Types = {"MAPREDUCE_JOB", "MAPREDUCE_TASK"};

        for (String Type : Entity_Types) {

            if (EntityType.equals(Type)) {
                return true;
            }

        }
        if (EntityType.matches("MAPREDUCE(.*)")) {
            return true;
        }

        return false;
    }

    /**
     * This method Check if the job he Tez type
     *
     * @param EntityType ,Entity Type as {@link String}
     * @return An {@link boolean}
     */
    private boolean if_Tezjob(String EntityType) {

        String[] Entity_Types = {"TEZ_APPLICATION",
            "TEZ_APPLICATION_ATTEMPT",
            "TEZ_CONTAINER_ID",
            "TEZ_DAG_ID",
            "TEZ_VERTEX_ID",
            "TEZ_TASK_ID"};

        for (String Type : Entity_Types) {
            if (EntityType.equals(Type)) {
                return true;
            }
        }

        if (EntityType.matches("TEZ(.*)")) {
            return true;
        }

        return false;
    }

    /**
     * This method Getting TimelineEntity While running job and check the type
     * is Mapreduce or Tez job And send entity to send Telemetry to Application
     * Insights
     *
     * @param entity ,Timeline Entity as type {@link TimelineEntity}
     */
    private void SendInfoToApplicationInsights(TimelineEntity entity) throws Exception {

        String EntityType = entity.getEntityType();

        if (if_MapReducejob(EntityType)) {
            Send_Mapreduce_Telemetry(entity);

        } else if (if_Tezjob(EntityType)) {
            Send_Tez_Telemetry(entity);

        } else {
            LOG.error(PATTERN_LOG_ERROR + String.format("Error : type of entity %s Not supported ", EntityType));
        }

    }

    private double get_SecondsDifferent(long startTime, long endTime) {
        Date start = new Date((long) startTime);
        Date end = new Date((long) endTime);

        long diff = end.getTime() - start.getTime();

        double diffSeconds = (double) diff / 1000;

        return (double) Math.round(diffSeconds);
    }

    /**
     * This method Getting TimelineEntity type of Tez job And send information
     * as Telemetry to Application Insights
     *
     * @param entity ,Timeline Entity as type {@link TimelineEntity}
     */
    private void Send_Tez_Telemetry(TimelineEntity entity) throws Exception {

        try {
            Map<String, String> properties = new HashMap<String, String>();
            Map<String, Double> metrics = new HashMap<String, Double>();
            EventTelemetry eventtelemetry = new EventTelemetry();
            List<TraceTelemetry> GroupCounters = new ArrayList<TraceTelemetry>();
            String Event_Name = "";
            String job_type = "TEZ";
            String job_status = "";
            String operation_id = "";
            String even_name = "";
            long startTime = 0;
            long endTime = 0;
            int timeTaken = 0;

            if (entity.getEntityId().matches("dag(.*)")) {
                operation_id = entity.getEntityId().replaceFirst("dag", "Application");
            } else if (entity.getEntityId().matches("vertex(.*)")) {
                operation_id = entity.getEntityId().replaceFirst("vertex", "Application");
            } else if (entity.getEntityId().matches("task(.*)")) {
                operation_id = entity.getEntityId().replaceFirst("task", "Application");
            }

            if (entity.getEvents() != null) {
                List<TimelineEvent> events = entity.getEvents();

                for (TimelineEvent event : events) {

                    Event_Name = event.getEventType();

                    LOG.info(PATTERN_LOG_INFO + String.format(" Event Type %s  Entity ID :%s   Entity Type : %s :", event.getEventType(), entity.getEntityId(), entity.getEntityType()));

                }
            } else {
                LOG.info(PATTERN_LOG_INFO + "No information about the event ");
            }

            if (entity.getOtherInfo().get("config") != null) {
                Map<String, String> config = (LinkedHashMap) entity.getOtherInfo().get("config");
                if (config != null) {
                    String value = "";
                    String value_from_config = null;
                    for (Map.Entry<String, String> entrySet : dimension_from_config.entrySet()) {

                        value = entrySet.getValue();
                        value_from_config = config.get(value);
                        if (value_from_config != null) {
                            dimension_to_sending.put(value, value_from_config);

                        } else {
                            dimension_to_sending.put(value, "not found");

                        }
                    }

                }
            }

            if ((Event_Name.equals("DAG_STARTED") || Event_Name.equals("DAG_FINISHED") || Event_Name.equals("VERTEX_STARTED") || Event_Name.equals("VERTEX_FINISHED")
                    || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED"))) {

                if (Event_Name.equals("DAG_STARTED")) {
                    job_id = entity.getEntityId().replaceFirst("dag", "Application");

                    String[] arr = job_id.split("_");

                    if (arr.length >= 3) {
                        job_id = String.format("%s_%s_%s", arr[0], arr[1], arr[2]);
                    }

                }

                if (entity.getOtherInfo() != null) {
                    Map<String, Object> OtherInfo = entity.getOtherInfo();

                    Set set = entity.getOtherInfo().entrySet();

                    if (set.size() > 0) {
                        WriteToFileLog("Start List of Other Info of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));
                    }
                    Iterator iter = set.iterator();

                    while (iter.hasNext()) {
                        // Display elements
                        Map.Entry me = (Map.Entry) iter.next();

                        switch ((String) me.getKey()) {

                            case "status":

                                if (Event_Name.equals("DAG_STARTED") || Event_Name.equals("DAG_FINISHED") || Event_Name.equals("VERTEX_STARTED") || Event_Name.equals("VERTEX_FINISHED")
                                        || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {

                                    job_status = (String) me.getValue();

                                    if (Event_Name.equals("DAG_FINISHED") || Event_Name.equals("TASK_FINISHED") || Event_Name.equals("VERTEX_FINISHED")) {

                                        properties.put("job_status", job_status);
                                    }
                                }
                                break;

                            case "endTime":
                            case "startTime":
                            case "timeTaken":

                                if (((String) me.getKey()).equals("startTime")) {
                                    startTime = (long) me.getValue();

                                } else if (((String) me.getKey()).equals("endTime")) {

                                    endTime = (long) me.getValue();
                                    eventtelemetry.setTimestamp(new Date(endTime));

                                } else if (((String) me.getKey()).equals("timeTaken")) {

                                    timeTaken = (int) me.getValue();
                                    double result = (double) timeTaken / 1000;
                                    metrics.put("Duration", result);

                                }

                                break;

                            case "stats":

                                if (me.getValue() instanceof LinkedHashMap) {
                                    LOG.info(PATTERN_LOG_INFO + String.format("tha value of stats : %s", me.getValue().getClass().getName()));
                                    // printLinkedHashMap(value);
                                    Map<Object, Object> linkedHashMap = (LinkedHashMap) me.getValue();

                                    for (Map.Entry<Object, Object> entrySet : linkedHashMap.entrySet()) {

                                        Object key = entrySet.getKey();
                                        Object value1 = entrySet.getValue();
                                        LOG.info(PATTERN_LOG_INFO + String.format(" key type %s : value type %s", (String) key, value1.getClass().getName()));
                                        if (value1 instanceof Integer) {
                                            LOG.info(PATTERN_LOG_INFO + String.format(" key(Integer) %s : value  %d", (String) key, (int) value1));
                                        } else if (value1 instanceof Double) {
                                            LOG.info(PATTERN_LOG_INFO + String.format(" key(Double) %s : value  %s", (String) key, Double.toString((double) value1)));
                                        } else if (value1 instanceof Long) {
                                            LOG.info(PATTERN_LOG_INFO + String.format(" key(Long) %s : value  %s", (String) key, CastFromTimeToStringFormat((long) value1)));
                                        }
                                        if (value1 instanceof ArrayList) {
                                            if (!((ArrayList) value1).isEmpty()) {
                                                int i = 1;
                                                Object[] ob = ((ArrayList) value1).toArray();

                                                for (Object part : ob) {
                                                    if (part instanceof String) {
                                                        LOG.info(PATTERN_LOG_INFO + String.format(" tha part %d of ArrayList type : %s", i++, (String) part));
                                                    }

                                                }

                                            }
                                        }
                                    }
                                }

                                break;
                            case "counters":

                                if (me.getValue() instanceof LinkedHashMap) {

                                    Map<Object, Object> Map = (LinkedHashMap) me.getValue();
                                    Object key;
                                    Object value1;
                                    for (Map.Entry<Object, Object> entrySet : Map.entrySet()) {

                                        key = entrySet.getKey();
                                        value1 = entrySet.getValue();

                                        if (value1 instanceof ArrayList) {

                                            if (!((ArrayList) value1).isEmpty()) {

                                                Object[] ob = ((ArrayList) value1).toArray();

                                                for (Object part : ob) {

                                                    if (part instanceof LinkedHashMap) {

                                                        Map<Object, Object> linkedHashMap = (LinkedHashMap) part;

                                                        TraceTelemetry tracetelemetry = new TraceTelemetry((String) linkedHashMap.get("counterGroupDisplayName"), SeverityLevel.Information);

                                                        tracetelemetry.getContext().getOperation().setId(operation_id);

                                                        for (Map.Entry<Object, Object> entrySetnext : linkedHashMap.entrySet()) {

                                                            Object key2 = entrySetnext.getKey();
                                                            Object value2 = entrySetnext.getValue();

                                                            if ((key2 instanceof String) && (value2 instanceof ArrayList)) {

                                                                Object[] ob1 = ((ArrayList) value2).toArray();

                                                                for (Object part1 : ob1) {

                                                                    if (part1 instanceof LinkedHashMap) {
                                                                        Map<Object, Object> linkedHashMap2 = (LinkedHashMap) part1;
                                                                        if (linkedHashMap2.get("counterValue") instanceof Integer) {
                                                                            tracetelemetry.getProperties().put((String) linkedHashMap2.get("counterName"), Integer.toString((int) linkedHashMap2.get("counterValue")));

                                                                        } else if (linkedHashMap2.get("counterValue") instanceof Long) {
                                                                            tracetelemetry.getProperties().put((String) linkedHashMap2.get("counterName"), Long.toString((long) linkedHashMap2.get("counterValue")));

                                                                        } else if (linkedHashMap2.get("counterValue") instanceof Double) {
                                                                            tracetelemetry.getProperties().put((String) linkedHashMap2.get("counterName"), Double.toString((double) linkedHashMap2.get("counterValue")));

                                                                        }
                                                                    }
                                                                }

                                                            }
                                                        }
                                                        GroupCounters.add(tracetelemetry);
                                                    }
                                                }
                                            }

                                        }
                                    }
                                } else {

                                    LOG.info(PATTERN_LOG_INFO + String.format("The data type %s is not supported in this version", me.getValue().getClass().getName()));
                                }

                                break;

                            default:
                                break;
                        }
                    }

                } else {
                    LOG.info(PATTERN_LOG_INFO + "No information about the Other Info ");
                }

                if (Event_Name.equals("DAG_STARTED") || Event_Name.equals("DAG_FINISHED") || Event_Name.equals("VERTEX_STARTED") || Event_Name.equals("VERTEX_FINISHED")
                        || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {

                    switch (Event_Name) {
                        case "DAG_STARTED":
                            even_name = "job_started";
                            break;
                        case "DAG_FINISHED":
                            even_name = "job_finished";

                            for (Map.Entry<String, String> entrySet : dimension_to_sending.entrySet()) {

                                properties.put(entrySet.getKey(), entrySet.getValue());
                                LOG.info(PATTERN_LOG_INFO + String.format("%s is  %s", entrySet.getKey(), entrySet.getValue()));
                            }
                            dimension_to_sending.clear();

                            break;
                        case "VERTEX_STARTED":
                        case "TASK_STARTED":
                            even_name = "task_started";
                            break;
                        case "VERTEX_FINISHED":
                        case "TASK_FINISHED":
                            even_name = "task_finished";
                            break;
                        default:

                            break;
                    }

                    LOG.info(String.format(PATTERN_LOG_INFO + "job_id is  %s", job_id));
                    LOG.info(String.format(PATTERN_LOG_INFO + "operation_id  is  %s", operation_id));
                    LOG.info(String.format(PATTERN_LOG_INFO + "job_type  is  %s", job_type));

                    properties.put("job_id", job_id);
                    properties.put("operation_id", operation_id);
                    properties.put("job_type", job_type);

                    for (Map.Entry<String, String> entrySet : dimension_to_sending.entrySet()) {

                        properties.put(entrySet.getKey(), entrySet.getValue());
                        LOG.info(PATTERN_LOG_INFO + String.format("%s is  %s", entrySet.getKey(), entrySet.getValue()));
                    }

                    eventtelemetry.getMetrics().putAll(metrics);
                    eventtelemetry.getProperties().putAll(properties);
                    eventtelemetry.setName(even_name);
                    eventtelemetry.getContext().getOperation().setId(operation_id);

                    telemetry.trackEvent(eventtelemetry);
                    for (TraceTelemetry GroupCounter : GroupCounters) {

                        telemetry.trackTrace(GroupCounter);
                    }

                    //telemetry.trackEvent(even_name, properties, metrics);
                }

            }
        } catch (Exception e) {
            String message = PATTERN_LOG_ERROR + "Creating a problem while send telemetry to Applocation insights......";
            LOG.error(message, e);
            throw new Exception(message);
        }
    }

    /**
     * This method Getting TimelineEntity type of Mapreduce job And send
     * information as Telemetry to Application Insights
     *
     * @param entity ,Timeline Entity as type {@link TimelineEntity}
     */
    private void Send_Mapreduce_Telemetry(TimelineEntity entity) throws Exception {

        try {

            Map<String, String> properties = new HashMap<String, String>();
            Map<String, Double> metrics = new HashMap<String, Double>();
            EventTelemetry eventtelemetry = new EventTelemetry();
            List<TraceTelemetry> GroupCounters = new ArrayList<TraceTelemetry>();
            String Event_Name = "";
            String job_type = "MAPREDUCE";
            String job_status = "";
            String operation_id = "";
            String even_name = "";
            Boolean if_send_event = true;

            if (entity.getEntityId().matches("job(.*)")) {
                operation_id = entity.getEntityId().replaceFirst("job", "Application");
            } else if (entity.getEntityId().matches("task(.*)")) {
                operation_id = entity.getEntityId().replaceFirst("task", "Application");
            }

            if (entity.getEvents() != null) {
                List<TimelineEvent> events = entity.getEvents();

                for (TimelineEvent event : events) {

                    Event_Name = event.getEventType();

                    LOG.info(PATTERN_LOG_INFO + String.format("Event Type %s  Entity ID :%s   Entity Type : %s :", event.getEventType(), entity.getEntityId(), entity.getEntityType()));

                }
            } else {
                LOG.info(PATTERN_LOG_INFO + "No information about the event");
            }

            if (Event_Name.equals("JOB_FINISHED") || Event_Name.equals("JOB_SUBMITTED") || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {

                if (Event_Name.equals("JOB_SUBMITTED")) {
                    job_id = entity.getEntityId().replaceFirst("job", "Application");
                    dimension_to_sending.clear();

                    long startTime = System.currentTimeMillis();
                    dimension_to_sending = get_properties_values(dimension_from_config, job_id);
                    long endTime = System.currentTimeMillis();
                    long duration = (endTime - startTime);

                    LOG.info(PATTERN_LOG_INFO + String.format("The duration of request http for information from server is %s (ms)", Long.toString(duration)));

                }

                if (entity.getEvents() != null) {
                    List<TimelineEvent> events = entity.getEvents();

                    for (TimelineEvent event : events) {

                        Set set = event.getEventInfo().entrySet();

                        if (set.size() > 0) {
                            WriteToFileLog("Start List of Other Info of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));
                        }
                        Iterator iter = set.iterator();

                        while (iter.hasNext()) {
                            // Display elements
                            Map.Entry me = (Map.Entry) iter.next();

                            switch ((String) me.getKey()) {

                                case "TASK_TYPE":
                                    if (Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {

                                        properties.put("task_type", (String) me.getValue());
                                    }
                                    break;
                                case "STATUS":
                                case "JOB_STATUS":

                                    if (Event_Name.equals("JOB_FINISHED") || Event_Name.equals("JOB_SUBMITTED") || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {
                                        job_status = (String) me.getValue();

                                        if (Event_Name.equals("JOB_FINISHED") || Event_Name.equals("TASK_FINISHED")) {
                                            properties.put("job_status", job_status);
                                        }
                                    }
                                    break;

                                case "START_TIME":
                                case "SUBMIT_TIME":
                                case "FINISH_TIME":

                                    if (Event_Name.equals("TASK_STARTED") && ((String) me.getKey()).equals("START_TIME")) {

                                        times.put(entity.getEntityId(), (long) me.getValue());

                                        eventtelemetry.setTimestamp(new Date((long) me.getValue()));

                                    } else if (Event_Name.equals("JOB_SUBMITTED") && ((String) me.getKey()).equals("SUBMIT_TIME")) {

                                        times.put(entity.getEntityId(), (long) me.getValue());
                                        eventtelemetry.setTimestamp(new Date((long) me.getValue()));

                                    } else if (Event_Name.equals("TASK_FINISHED") || Event_Name.equals("JOB_FINISHED")) {
                                        if (!times.isEmpty()) {

                                            if (times.get(entity.getEntityId()) != null) {

                                                long start_time = times.get(entity.getEntityId());

                                                long end_time = (long) me.getValue();

                                                double Duration = get_SecondsDifferent(start_time, end_time);

                                                metrics.put("Duration", Duration);

                                                times.remove(entity.getEntityId());

                                                eventtelemetry.setTimestamp(new Date(end_time));

                                            } else {
                                                LOG.error(PATTERN_LOG_ERROR + String.format("not founf start time of task %s in map times ", entity.getEntityId()));
                                                if_send_event = false;
                                            }
                                        } else {
                                            LOG.error(PATTERN_LOG_ERROR + String.format("not founf start time of task %s in map times the map is Empty", entity.getEntityId()));
                                            if_send_event = false;
                                        }
                                    }

                                    break;

                                case "TOTAL_COUNTERS_GROUPS":
                                case "COUNTERS_GROUPS":
                                case "REDUCE_COUNTERS_GROUPS":
                                case "MAP_COUNTERS_GROUPS":
                                    if (me.getValue() instanceof ArrayList) {

                                        if (!((ArrayList) me.getValue()).isEmpty()) {

                                            Object[] ob = ((ArrayList) me.getValue()).toArray();

                                            for (Object part : ob) {

                                                if (part instanceof LinkedHashMap) {

                                                    Map<Object, Object> linkedHashMap = (LinkedHashMap) part;

                                                    TraceTelemetry tracetelemetry = null;
                                                    if (((String) me.getKey()).equals("TOTAL_COUNTERS_GROUPS")) {
                                                        tracetelemetry = new TraceTelemetry("total job finished " + (String) linkedHashMap.get("DISPLAY_NAME"), SeverityLevel.Information);
                                                    } else if (((String) me.getKey()).equals("REDUCE_COUNTERS_GROUPS")) {
                                                        tracetelemetry = new TraceTelemetry("reduce job finished " + (String) linkedHashMap.get("DISPLAY_NAME"), SeverityLevel.Information);
                                                    } else if (((String) me.getKey()).equals("MAP_COUNTERS_GROUPS")) {
                                                        tracetelemetry = new TraceTelemetry("map job finished " + (String) linkedHashMap.get("DISPLAY_NAME"), SeverityLevel.Information);
                                                    } else {
                                                        tracetelemetry = new TraceTelemetry("task finished " + (String) linkedHashMap.get("DISPLAY_NAME"), SeverityLevel.Information);
                                                    }

                                                    tracetelemetry.getContext().getOperation().setId(operation_id);

                                                    for (Map.Entry<Object, Object> entrySet : linkedHashMap.entrySet()) {

                                                        Object key = entrySet.getKey();
                                                        Object value1 = entrySet.getValue();

                                                        if ((key instanceof String) && (value1 instanceof ArrayList)) {

                                                            Object[] ob1 = ((ArrayList) value1).toArray();

                                                            for (Object part1 : ob1) {

                                                                if (part1 instanceof LinkedHashMap) {
                                                                    Map<Object, Object> linkedHashMap2 = (LinkedHashMap) part1;

                                                                    if (linkedHashMap2.get("VALUE") instanceof Integer) {

                                                                        tracetelemetry.getProperties().put((String) linkedHashMap2.get("NAME"), String.format("%d", (Integer) linkedHashMap2.get("VALUE")));

                                                                    }

                                                                }
                                                            }

                                                        } else if ((key instanceof String) && (value1 instanceof String)) {

                                                        } else if ((key instanceof String) && (value1 instanceof Integer)) {

                                                        }
                                                    }
                                                    GroupCounters.add(tracetelemetry);
                                                }
                                            }
                                        }

                                    } else {

                                        LOG.info(PATTERN_LOG_INFO + String.format("The data type %s is not supported in this version", me.getValue().getClass().getName()));
                                    }
                                    break;

                                default:
                                    break;
                            }
                        }
                    }

                } else {
                    LOG.info(PATTERN_LOG_INFO + "No information about event ");
                }

                if (Event_Name.equals("JOB_FINISHED") || Event_Name.equals("JOB_SUBMITTED") || Event_Name.equals("TASK_STARTED") || Event_Name.equals("TASK_FINISHED")) {

                    switch (Event_Name) {
                        case "JOB_SUBMITTED":
                            even_name = "job_started";
                            break;
                        case "JOB_FINISHED":
                            even_name = "job_finished";

                            for (Map.Entry<String, String> entrySet : dimension_to_sending.entrySet()) {

                                properties.put(entrySet.getKey(), entrySet.getValue());
                                LOG.info(PATTERN_LOG_INFO + String.format("%s is  %s", entrySet.getKey(), entrySet.getValue()));

                            }
                            dimension_to_sending.clear();
                            break;
                        case "TASK_STARTED":
                            even_name = "task_started";
                            break;
                        case "TASK_FINISHED":
                            even_name = "task_finished";
                            break;
                        default:
                            break;
                    }

                    LOG.info(PATTERN_LOG_INFO + String.format("job_id is  %s", job_id));
                    LOG.info(PATTERN_LOG_INFO + String.format("operation_id is  %s", operation_id));
                    LOG.info(PATTERN_LOG_INFO + String.format("job_type is  %s", job_type));

                    properties.put("job_id", job_id);
                    properties.put("operation_id", operation_id);
                    properties.put("job_type", job_type);

                    for (Map.Entry<String, String> entrySet : dimension_to_sending.entrySet()) {

                        properties.put(entrySet.getKey(), entrySet.getValue());
                        LOG.info(PATTERN_LOG_INFO + String.format("%s is  %s", entrySet.getKey(), entrySet.getValue()));
                    }

                    eventtelemetry.getMetrics().putAll(metrics);
                    eventtelemetry.getProperties().putAll(properties);
                    eventtelemetry.setName(even_name);
                    eventtelemetry.getContext().getOperation().setId(operation_id);

                    if (if_send_event) {
                        telemetry.trackEvent(eventtelemetry);

                        // telemetry.trackEvent(even_name, properties, metrics);
                    }

                    for (TraceTelemetry GroupCounter : GroupCounters) {

                        telemetry.trackTrace(GroupCounter);
                    }

                }

            }
        } catch (Exception e) {
            String message = PATTERN_LOG_ERROR + "Creating a problem while send telemetry to Applocation insights......";
            LOG.error(message, e);
            throw new Exception(message);
        }
    }

    private void printLinkedHashMap(Object value) {

        Map<Object, Object> linkedHashMap = (LinkedHashMap) value;

        for (Map.Entry<Object, Object> entrySet : linkedHashMap.entrySet()) {

            Object key = entrySet.getKey();
            Object value1 = entrySet.getValue();

            if ((key instanceof String) && (value1 instanceof ArrayList)) {

                int i = 0;
                if (!((ArrayList) value1).isEmpty()) {

                    Object[] ob1 = ((ArrayList) value1).toArray();
                    i = ob1.length;
                    for (Object part1 : ob1) {
                        if (part1 instanceof String) {

                            LOG.info(PATTERN_LOG_INFO + String.format(" the part %d of ArrayList type : %s", i++, (String) part1));

                        } else if (part1 instanceof LinkedHashMap) {
                            printLinkedHashMap(part1);

                        } else {
                            LOG.info(PATTERN_LOG_INFO + String.format(" the part %d of ArrayList type : %s", i++, part1.getClass().getName()));
                        }

                    }
                }
                LOG.info(PATTERN_LOG_INFO + String.format(" key(string)  %s : value(ArrayList)   %d", (String) key, i));

            } else if ((key instanceof String) && (value1 instanceof String)) {

                LOG.info(PATTERN_LOG_INFO + String.format(" key(string)  %s : value(string)  %s", (String) key, (String) value1));

            } else if ((key instanceof String) && (value1 instanceof Integer)) {

                LOG.info(PATTERN_LOG_INFO + String.format(" key(String)  %s : value(Integer)  %d", (String) key, (int) value1));

            } else if ((key instanceof String) && (value1 instanceof LinkedHashMap)) {

                LOG.info(PATTERN_LOG_INFO + String.format(" key(string)  %s : value type  %s", (String) key, value1.getClass().getName()));

                printLinkedHashMap(value1);

            } else if ((key instanceof String) && (value1 instanceof Long)) {

                LOG.info(PATTERN_LOG_INFO + String.format(" key(String)  %s : value(Long)  %s", (String) key, CastFromTimeToStringFormat((long) value1)));

            } else if ((key instanceof String) && (value1 instanceof Double)) {

                LOG.info(PATTERN_LOG_INFO + String.format(" key(String)  %s : value(double)  %s", (String) key, Double.toString((double) value1)));

            } else {

                LOG.info(PATTERN_LOG_INFO + String.format(" key type %s : value type %s", key.getClass().getName(), value1.getClass().getName()));
            }
        }
    }

    private void WriteObjectToLog(String str, Object value) throws Exception {
        try {

            if (!str.equals("")) {
                LOG.info(PATTERN_LOG_INFO + str);
            }

            if (value instanceof String) {
                LOG.info(PATTERN_LOG_INFO + (String) value);
            } else if (value instanceof Long) {

                LOG.info(PATTERN_LOG_INFO + CastFromTimeToStringFormat((long) value));

            } else if (value instanceof Integer) {
                LOG.info(PATTERN_LOG_INFO + String.format("%d", (int) value));
            } else if (value instanceof Boolean) {
                LOG.info(PATTERN_LOG_INFO + Boolean.toString((boolean) value));
            } else if (value instanceof Double) {
                LOG.info(PATTERN_LOG_INFO + Double.toString((double) value));
            } else if (value instanceof Float) {
                LOG.info(PATTERN_LOG_INFO + String.format("%f", (float) value));
            } else if (value instanceof Short) {
                LOG.info(PATTERN_LOG_INFO + Short.toString((short) value));
            } else if (value instanceof ArrayList) {
                LOG.info(PATTERN_LOG_INFO + String.format("tha value type : %s", value.getClass().getName()));
                if (!((ArrayList) value).isEmpty()) {
                    int i = 1;
                    Object[] ob = ((ArrayList) value).toArray();

                    for (Object part : ob) {
                        LOG.info(PATTERN_LOG_INFO + String.format(" tha part %d of ArrayList type : %s", i++, part.getClass().getName()));

                        if (part instanceof LinkedHashMap) {
                            //WriteObjectToLog("", part); 
                            printLinkedHashMap(part);
                        }
                    }

                }
            } else if (value instanceof LinkedHashMap) {
                LOG.info(String.format(PATTERN_LOG_INFO + "tha value type : %s", value.getClass().getName()));
                printLinkedHashMap(value);

            } else if (value instanceof HashSet) {

                LOG.info(PATTERN_LOG_INFO + String.format("tha value type : %s", value.getClass().getName()));

            } else {
                LOG.info(PATTERN_LOG_INFO + String.format("(not case )tha object type : %s", value.getClass().getName()));
            }

            if (!str.equals("")) {
                LOG.info(PATTERN_LOG_INFO + "}");
            }

        } catch (Exception e) {

            String message = PATTERN_LOG_ERROR + "Creating a problem while writing the history to Log file";
            LOG.error(message, e);
            throw e;
        }
    }

    /*
     ** This function gets TimelineEntity object With lots of information 
     ** Prints to Log all information about TimelineEntity,
     ** 
     */
    private void putToLog(TimelineEntity entity) throws Exception {

        try {

            WriteToFileLog("Entity Type  :", entity.getEntityType());

            WriteToFileLog("Entity ID  :", entity.getEntityId());

            WriteToFileLog("Domain ID  :", entity.getDomainId());

            if (entity.getStartTime() != null) {
                WriteToFileLog("Start Time :", CastFromTimeToStringFormat(entity.getStartTime()));
            }

            // print all events of Entity
            WriteToFileLog("\tprint all events   ", "");

            LOG.info(PATTERN_LOG_INFO + " print all events of Entity Name :" + String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

            if (entity.getEvents() != null) {
                List<TimelineEvent> events = entity.getEvents();

                WriteToFileLog("Start List of events of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

                for (TimelineEvent event : events) {

                    WriteToFileLog("\tEvent Type  :", event.getEventType() + ",");
                    WriteToFileLog("\tTime Stamp  :", CastFromTimeToStringFormat(event.getTimestamp()) + ",");

                    if (event.getEventInfo() != null) {
                        WriteToFileLog("\tprint all Event Info  ", "");
                        // Get an iterator
                        Set set = event.getEventInfo().entrySet();

                        Iterator iter = set.iterator();

                        if (set.size() > 0) {
                            WriteToFileLog("\t\tStart List Event Info  ", "");
                        }

                        while (iter.hasNext()) {
                            // Display elements
                            Map.Entry me = (Map.Entry) iter.next();

                            //sed information to history file
                            WriteObjectToLog("\t\t { Key : " + me.getKey() + ", Value: ", me.getValue());

                        }
                        if (set.size() > 0) {
                            WriteToFileLog("\n\t\tEnd List Event Info  ", "");
                        }
                    }

                }
                WriteToFileLog("End List of events of  :", entity.getEntityType() + "\n\n");
            } else {
                LOG.info(PATTERN_LOG_INFO + "variable  events of entity is null ");
            }

            // print all Related Entities of Entity
            WriteToFileLog("\tprint all Related Entities   ", "");
            LOG.info(PATTERN_LOG_INFO + "print all Related Entities of Entity Name :" + String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));
            if (entity.getRelatedEntities() != null) {
                //Map<String, Set<String>> RelatedEntities = entity.getRelatedEntities();

                WriteToFileLog("Start List of Related Entities of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

                Set set = entity.getRelatedEntities().entrySet();

                Iterator iter = set.iterator();

                if (set.size() > 0) {
                    WriteToFileLog("\t\tStart List Related Entities  ", "");
                }
                while (iter.hasNext()) {
                    // Display elements
                    Map.Entry me = (Map.Entry) iter.next();

                    //send information to history file
                    Set<String> setcoll = (Set<String>) me.getValue();

                    if (setcoll.size() > 0) {
                        WriteToFileLog("\t\t { Key: (" + me.getKey() + " )  ", "\n\t\t\t[");
                    } else {
                        WriteToFileLog("\t\t { Key: (" + me.getKey() + " )  ", "\n\t\t\t Empty Value");
                    }
                    //over all parts of set<string>
                    for (String str : setcoll) {
                        WriteToFileLog("\t\t\t", str + ",");
                    }
                    WriteToFileLog("\n\t\t\t]", "\n}");

                }
                if (set.size() > 0) {
                    WriteToFileLog("\t\tEnd List Related Entities  ", "");
                }
                WriteToFileLog("\t\t}", "");
                WriteToFileLog("End List of Related Entities  of  :", entity.getEntityType() + "\n\n");

            } else {
                LOG.info(PATTERN_LOG_INFO + "variable  Related Entities is null ");
            }

            // print all Primary Filters of Entity
            WriteToFileLog("\tprint all Primary Filters   ", "");
            LOG.info(PATTERN_LOG_INFO + "print all Primary Filters of Entity Name :" + String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

            if (entity.getPrimaryFilters() != null) {
                //Map<String, Set<Object>> PrimaryFilters = entity.getRelatedEntities();

                WriteToFileLog("Start List of Primary Filters of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

                Set set = entity.getPrimaryFilters().entrySet();

                Iterator iter = set.iterator();

                if (set.size() > 0) {
                    WriteToFileLog("\t\tStart List Primary Filters   ", "");
                }
                while (iter.hasNext()) {
                    // Display elements
                    Map.Entry me = (Map.Entry) iter.next();

                    //sed information to history file
                    Set<Object> setcoll = (Set<Object>) me.getValue();

                    if (setcoll.size() > 0) {
                        WriteToFileLog("\t\t { Key: (" + me.getKey() + " )  ", "\n\t\t\t[");
                    } else {
                        WriteToFileLog("\t\t { Key: (" + me.getKey() + " )  ", "\n\t\t\t Empty Value");
                    }
                    //over all parts of set<Object>
                    for (Object obj : setcoll) {
                        WriteObjectToLog("", obj);
                    }
                    WriteToFileLog("\n\t\t\t]", "\n}");

                }
                if (set.size() > 0) {
                    WriteToFileLog("\t\tEnd List Related Entities  ", "");
                }
                WriteToFileLog("\t\t}", "");
                WriteToFileLog("End List of Primary Filters  of  :", entity.getEntityType() + "\n\n");

            } else {
                LOG.info(PATTERN_LOG_INFO + "variable  Primary Filters is null ");
            }

            // print all Other Info of Entity
            WriteToFileLog("\tprint all Other Info   ", "");
            LOG.info(PATTERN_LOG_INFO + "print all Other Info of Entity Name :" + String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));

            if (entity.getOtherInfo() != null) {
                // Map<String,Object> OtherInfo = entity.getOtherInfo();

                Set set = entity.getOtherInfo().entrySet();

                if (set.size() > 0) {
                    WriteToFileLog("Start List of Other Info of  ( ", String.format("Entity ID :%s , Entity Type : %s )\n\n", entity.getEntityId(), entity.getEntityType()));
                }
                Iterator iter = set.iterator();

                while (iter.hasNext()) {
                    // Display elements
                    Map.Entry me = (Map.Entry) iter.next();

                    //sed information to history file
                    WriteObjectToLog("\t Key : " + me.getKey() + "   : Value : ", me.getValue());
                }
                if (set.size() > 0) {
                    WriteToFileLog("\nEnd List of Other Info  of  :", entity.getEntityType() + "\n\n");
                }

            } else {
                LOG.info(PATTERN_LOG_INFO + "variable  Other Info is null ");
            }

            LOG.info(PATTERN_LOG_INFO + "Finished to print all information of Entity Name :" + String.format("Entity ID :%s , Entity Type : %s )\n\n\n\n", entity.getEntityId(), entity.getEntityType()));

        } catch (Exception e) {

            String message = PATTERN_LOG_ERROR + "Creating a problem while writing the history  file";
            LOG.error(message, e);

            throw e;
        } finally {

        }
    }

    /**
     * This method retrieves a list of entity information,
     * {@link TimelineEntity}, sorted by the starting timestamp for the entity,
     * descending. The starting timestamp of an entity is a timestamp specified
     * by the client. If it is not explicitly specified, it will be chosen by
     * the store to be the earliest timestamp of the events received in the
     * first put for the entity.
     *
     * @return An {@link TimelineEntities} object.
     * @throws IOException
     *
     * the function For version 2.7 of hadoop
     */
    @Override
    public TimelineEntities getEntities(String entityType,
            Long limit, Long windowStart, Long windowEnd, String fromId, Long fromTs,
            NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
            EnumSet<Field> fieldsToRetrieve, TimelineDataManager.CheckAcl ca) throws IOException {

        TimelineEntities timelineEntities = null;
        try {
            if (originalStorage != null) {
                timelineEntities = originalStorage.getEntities(entityType, limit, windowStart, windowEnd, fromId, fromTs, primaryFilter, secondaryFilters, fieldsToRetrieve, ca);
            }
        } catch (IOException iOException) {
            LOG.error(PATTERN_LOG_ERROR + "Creating a problem to get timeline Entitys from Timeline Storage", iOException);
        }
        return timelineEntities;
    }

    // the function For version 2.6 of hadoop
    /*@Override
     public TimelineEntities getEntities(String string, Long l, Long l1, Long l2, String string1, Long l3, NameValuePair nvp, Collection<NameValuePair> clctn, EnumSet<Field> es) throws IOException {
     // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    
     TimelineEntities timelineEntities = null;
     try {
     if (originalStorage != null) {
     timelineEntities = originalStorage.getEntities(string, l, l1, l2, string1, l3, nvp, clctn, es);
     }
    
     } catch (IOException iOException) {
     LOG.error(PATTERN_LOG_ERROR+"Creating a problem to get timeline Entitys from Timeline Storage",iOException);
     }
     return timelineEntities;
     }*/
    /**
     * This method retrieves the entity information for a given entity.
     *
     * @return An {@link TimelineEntity} object.
     * @throws IOException
     */
    @Override
    public TimelineEntity getEntity(String entityId, String entityType, EnumSet<Field> fieldsToRetrieve) throws IOException {

        TimelineEntity timelineEntity = null;
        try {
            if (originalStorage != null) {
                timelineEntity = originalStorage.getEntity(entityId, entityType, fieldsToRetrieve);
            }

        } catch (IOException iOException) {

            LOG.error(PATTERN_LOG_ERROR + "Creating a problem to get Entity from Timeline Storage", iOException);
        }
        return timelineEntity;

    }

    /**
     * This method retrieves the events for a list of entities all of the same
     * entity type. The events for each entity are sorted in order of their
     * timestamps, descending.
     *
     * @return An {@link TimelineEvents} object.
     * @throws IOException
     */
    @Override
    public TimelineEvents getEntityTimelines(String entityType,
            SortedSet<String> entityIds, Long limit, Long windowStart,
            Long windowEnd, Set<String> eventTypes) throws IOException {

        TimelineEvents timelineEvents = null;
        try {
            if (originalStorage != null) {
                timelineEvents = originalStorage.getEntityTimelines(entityType, entityIds, limit, windowStart, windowEnd, eventTypes);
            }

        } catch (IOException iOException) {

            LOG.error(PATTERN_LOG_ERROR + " Creating a problem to get timeline Entitys from Timeline Storage", iOException);
        }
        return timelineEvents;
    }

    /**
     * This method retrieves the domain information for a given ID.
     *
     * @return a {@link TimelineDomain} object.
     * @throws IOException
     */
    @Override
    public TimelineDomain getDomain(String domainId) throws IOException {

        TimelineDomain td = null;
        try {

            if (originalStorage != null) {
                td = originalStorage.getDomain(domainId);
            }
        } catch (IOException e) {

            LOG.error(PATTERN_LOG_ERROR + "Creating a problem to get Timeline Domain from Timeline Storage", e);
        }
        return td;
    }

    /**
     * This method retrieves all the domains that belong to a given owner. The
     * domains are sorted according to the created time firstly and the modified
     * time secondly in descending order.
     *
     * @param owner the domain owner
     * @return an {@link TimelineDomains} object.
     * @throws IOException
     */
    @Override
    public TimelineDomains getDomains(String owner) throws IOException {

        TimelineDomains tds = null;
        try {
            if (originalStorage != null) {
                tds = originalStorage.getDomains(owner);
            }
        } catch (IOException e) {

            LOG.error(PATTERN_LOG_ERROR + "Creating a problem to get Timeline Domains from Timeline Storage", e);
        }
        return tds;
    }

    /**
     * This method is Main of My Project , this IntermediateHistoryStore class
     * extends TimelineStore of yarn and Override all function To pass the
     * information of each job to Timeline Storage original And This project
     * takes the Intermediate information and pulls out What is important and
     * sends the information as Telemetry to Application Insights Any errors
     * occurring for individual put request objects will be reported in the
     * response.
     *
     * @param data a {@link TimelineEntities} object.
     * @return a {@link TimelinePutResponse} object.
     * @throws IOException
     */
    @Override
    public TimelinePutResponse put(TimelineEntities data) throws IOException {

        TimelinePutResponse response = null;

        try {

            if (originalStorage != null) {
                response = originalStorage.put(data);
            }

            if (data != null) {
                WriteToFileLog("Start write  Histoey to File  :", "not null te and size of te " + String.format("%d", data.getEntities().size()));
                if (data.getEntities().size() > 0) {
                    for (TimelineEntity entity : data.getEntities()) {

                        if (entity != null) {

                            SendInfoToApplicationInsights(entity);
                            // putToLog(entity);                           
                        }
                    }
                }
            }

        } catch (IOException e) {

            String message = PATTERN_LOG_ERROR + "Creating a problem while send TimelineEntity ";
            LOG.error(message, e);

        } catch (Exception ex) {

            String message = PATTERN_LOG_ERROR + "Creating a problem while send TimelineEntity ";
            LOG.error(message, ex);
        }

        return response;
    }

    /**
     * Store domain information to the Timeline store. If A domain of the same
     * ID already exists in the Timeline store, it will be COMPLETELY updated
     * with the given domain.
     *
     * @param domain a {@link TimelineDomain} object
     * @throws IOException
     */
    @Override
    public void put(TimelineDomain domain) throws IOException {

        try {
            if (originalStorage != null) {
                originalStorage.put(domain);
            }
        } catch (IOException iOException) {

            LOG.error(PATTERN_LOG_ERROR + "Creating a problem reading the function put(TimelineDomain td) :", iOException);

            iOException.printStackTrace();
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {

        try {

            //Method m = originalStorage.getClass().getDeclaredMethod("serviceInit", Configuration.class);
            Method m = originalStorage.getClass().getMethod("serviceInit", Configuration.class);
            m.setAccessible(true);
            m.invoke(originalStorage, (Object) conf);

        } catch (NoSuchMethodException noSuchMethodException) {

            LOG.error(PATTERN_LOG_ERROR + "no Such Method Exception :", noSuchMethodException);
            noSuchMethodException.printStackTrace();

        } catch (SecurityException securityException) {

            LOG.error(PATTERN_LOG_ERROR + "Security Exception :", securityException);
            securityException.printStackTrace();

        } catch (IllegalAccessException illegalAccessException) {

            LOG.error(PATTERN_LOG_ERROR + "Illegal Access Exception :", illegalAccessException);
            illegalAccessException.printStackTrace();

        } catch (IllegalArgumentException illegalArgumentException) {

            LOG.error(PATTERN_LOG_ERROR + "Illegal Argument Exception :", illegalArgumentException);
            illegalArgumentException.printStackTrace();

        } catch (InvocationTargetException invocationTargetException) {

            Throwable cause = invocationTargetException.getCause();
            LOG.error(PATTERN_LOG_ERROR + "Invocation Target Exception failed because of:" + cause.getMessage(), invocationTargetException);
            invocationTargetException.printStackTrace();

        }
    }

    @Override
    protected void serviceStop() throws Exception {

        Method m;
        try {
            m = originalStorage.getClass().getDeclaredMethod("serviceStop", null);
            m.setAccessible(true);
            m.invoke(originalStorage, null);

        } catch (NoSuchMethodException noSuchMethodException) {

            LOG.error(PATTERN_LOG_ERROR + "no Such Method Exception :", noSuchMethodException);

        } catch (SecurityException securityException) {

            LOG.error(PATTERN_LOG_ERROR + "Security Exception :", securityException);

        } catch (IllegalAccessException illegalAccessException) {

            LOG.error(PATTERN_LOG_ERROR + "Illegal Access Exception :", illegalAccessException);

        } catch (IllegalArgumentException illegalArgumentException) {

            LOG.error(PATTERN_LOG_ERROR + "Illegal Argument Exception :", illegalArgumentException);

        } catch (InvocationTargetException invocationTargetException) {

            Throwable cause = invocationTargetException.getCause();
            LOG.error(PATTERN_LOG_ERROR + "Invocation Target Exception failed because of:" + cause.getMessage(), invocationTargetException);

        }
    }

    @Override
    public void start() {

        originalStorage.start();
        client.start();
    }

    @Override
    public void stop() {

        originalStorage.stop();
        client.stop();
    }

}
