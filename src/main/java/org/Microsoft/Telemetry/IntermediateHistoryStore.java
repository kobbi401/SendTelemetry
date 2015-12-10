/*ApplicationInsights-Java
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
import com.microsoft.applicationinsights.telemetry.ExceptionTelemetry;
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
import org.apache.hadoop.yarn.server.timeline.TimelineReader;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.slf4j.LoggerFactory;

/**
 * this Class to send History intermediate of Job to Application Insights , Add
 * also preserved in history in class of History storage of Yarn Timeline server
 *
 * @author b-yaif
 */
public class IntermediateHistoryStore extends AbstractService
        implements TimelineStore {

    private ServiceInformation serviceInformation = null;

    private static final Log LOG = LogFactory.getLog(IntermediateHistoryStore.class);
    private static final org.slf4j.Logger LOG2 = LoggerFactory.getLogger(IntermediateHistoryStore.class);  
    
    private String PATTERN_LOG_INFO = " [ Telemetry LI ] ";
    private String PATTERN_LOG_ERROR = " [ Telemetry LE ] ";
  
    private TimelineStore originalStorage = null;  
    private Configuration config = new YarnConfiguration();
   

     //Constructor overload
    public IntermediateHistoryStore(String name) throws YarnException, IOException {
        super(IntermediateHistoryStore.class.getName());

        try {
            serviceInformation=new ServiceInformation();
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
            serviceInformation=new ServiceInformation();
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

        if (serviceInformation != null) {
            this.serviceInformation.init(conf);
        } else {
            LOG.info(PATTERN_LOG_INFO + " serviceInformation not initialized");
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

        serviceInformation.initialization(conf);
        
        if (originalStorage != null) {

            LOG.info(PATTERN_LOG_INFO + "Timeline server Storage initialized successfully....!");
            LOG2.info(PATTERN_LOG_INFO + "Timeline server Storage initialized successfully....!");

        } else {
            LOG.error(PATTERN_LOG_ERROR + "Timeline server Storage Initialization failed");
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
            EnumSet<TimelineReader.Field> fieldsToRetrieve, TimelineDataManager.CheckAcl ca) throws IOException {

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
     LOG.error(PATTERN_LOG_ERROR + "Creating a problem to get timeline Entitys from Timeline Storage", iOException);
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
    public TimelineEntity getEntity(String entityId, String entityType, EnumSet<TimelineReader.Field> fieldsToRetrieve) throws IOException {

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
                
                if (data.getEntities().size() > 0) {
                    for (TimelineEntity entity : data.getEntities()) {

                        if (entity != null) {

                            serviceInformation.SendInfoToApplicationInsights(entity);
                            //putToLog(entity);
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
        serviceInformation.start();
    }

    @Override
    public void stop() {

        originalStorage.stop();
        serviceInformation.stop();
    }

}
