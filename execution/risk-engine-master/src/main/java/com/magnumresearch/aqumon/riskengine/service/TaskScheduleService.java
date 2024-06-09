package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.*;
import com.magnumresearch.aqumon.riskengine.event.TaskJob;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.quartz.CronScheduleBuilder.cronSchedule;

@Service
public class TaskScheduleService {
    @Autowired
    Scheduler scheduler;
    @Autowired
    BaseSchedulesConfig baseSchedulesConfig;

    public static final String GROUPNAME = "risk";
    public static final String JOBNAME = "jobName";
    public static final String SCHEDULERULE = "scheduleRule";
    public static final String CRONSCHEDULE = "cronSchedule";

    Logger log = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void init() {
        log.info("[RiskEngine][Schedule] on startup scheduling starts");
        try {
            List<JobDataMap> jobDataMaps = createJobDataMap();
            for (JobDataMap jobDataMap : jobDataMaps)
                schedule(jobDataMap);
            listScheduledJobs();
        }
        catch (Exception e) {
            log.error("[RiskEngine][Schedule] on startup scheduling failed: ", e);
        }
        log.info("[RiskEngine][Schedule] on startup scheduling completed");
    }

    /**
     * schedule jobs
     * @param jobDataMap containing job details
     */
    public void schedule(JobDataMap jobDataMap) {
        try {
            JobDetail jobDetail = buildJobDetail(jobDataMap);
            Trigger trigger = buildJobTrigger(jobDetail);
            if (scheduler.checkExists(jobDetail.getKey()))
                scheduler.scheduleJob(trigger);
            else
                scheduler.scheduleJob(jobDetail, trigger);
            } catch (SchedulerException e) {
            log.error("[RiskEngine][Schedule] schedule job failed: JobName=" + jobDataMap.getString(JOBNAME) + "\n", e);
        }
    }

    /**
     * build job details object
     * @param jobDataMap a map containing job data
     * @return job details object
     */
    private JobDetail buildJobDetail(JobDataMap jobDataMap) {
        return JobBuilder.newJob(TaskJob.class)
                .withIdentity(jobDataMap.getString(JOBNAME), "Risk")
                .usingJobData(jobDataMap)
                .build();
    }

    /**
     * build up job trigger
     * @param jobDetail job details object
     * @return job trigger based on job details
     */
    private Trigger buildJobTrigger(JobDetail jobDetail) {
        return TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(jobDetail.getKey().getName(), "risk-triggers")
                .withSchedule(cronSchedule(jobDetail.getJobDataMap().getString(CRONSCHEDULE)))
                .build();
    }

    /**
     * list all scheduled jobs
     * @return a map of job key and next job trigger time
     */
    public Map<String, String> listScheduledJobs() {
        log.info("[RiskEngine][Schedule] list scheduled jobs:");
        Map<String, String> resultMap = new TreeMap<>();
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();
                    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
                    if (triggers.size() > 0) {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        Date nextFireTime = triggers.get(0).getNextFireTime();
                        log.info("[RiskEngine][Schedule] list scheduled jobs: RiskGroup=" + jobGroup + ", JobName=" + jobName + ", NextFireTime=" + nextFireTime);
                        if (Objects.isNull(nextFireTime))
                            resultMap.put(jobName, "");
                        else {
                            resultMap.put(jobName, dateFormat.format(nextFireTime));
                        }
                    }
                    else
                        log.warn("[RiskEngine][Schedule] no scheduled trigger found: RiskGroup=" + jobGroup + ", JobName=" + jobName);
                }
            }
        } catch (SchedulerException schedulerException) {
            log.error("[RiskEngine][Schedule] schedule failed: failed to get scheduler ", schedulerException);
        }
        log.info("[RiskEngine][Schedule] list scheduled jobs completed");
        return resultMap;
    }

    /**
     * clear schedule job based on job key
     * @param jobKey job key
     */
    public void clearScheduledJob(JobKey jobKey) {
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName));
                if (jobKeys.contains(jobKey)) {
                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();
                    scheduler.deleteJob(jobKey);
                    log.info("[RiskEngine][Schedule] removed scheduled job: RiskGroup=" + jobGroup + ", JobName=" + jobName);
                }
            }
        } catch (SchedulerException schedulerException) {
            log.error("[RiskEngine][Schedule] schedule failed: failed to get scheduler ", schedulerException);
        }
    }

    /**
     * clear schedule jobs
     * @return a map of job key and next job trigger time
     */
    public Map<String, String> clearScheduledJobs() {
        log.info("[RiskEngine][Schedule] clear scheduled jobs starts");
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();
                    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
                    Date nextFireTime = triggers.get(0).getNextFireTime();
                    scheduler.deleteJob(jobKey);
                    log.info("[RiskEngine][Schedule] removed scheduled jobs: RiskGroup=" + jobGroup + ", JobName=" + jobName + ", NextFireTime=" + nextFireTime);
                }
            }
        } catch (SchedulerException schedulerException) {
            log.error("[RiskEngine][Schedule] schedule failed: failed to get scheduler ", schedulerException);
        }
        log.info("[RiskEngine][Schedule] clear scheduled jobs completed");
        return listScheduledJobs();
    }

    /**
     * update scheduled jobs based on a list of job data map objects
     * @param jobDataMapList a list of job data map objects
     * @return a map of job key and next job trigger time
     */
    public Map<String, String> updateScheduledJobs(List<JobDataMap> jobDataMapList){
        log.info("[RiskEngine][Schedule] update scheduled jobs starts");
        Map<String, JobDataMap> map = new HashMap<>();
        if (Objects.isNull(jobDataMapList) || jobDataMapList.size() == 0) {
            log.error("[RiskEngine][Schedule] scheduled jobs update failed: input jobDataMapList is null");
            return listScheduledJobs();
        }

        for (JobDataMap jobDataMap: jobDataMapList) {
            map.put(jobDataMap.getString(JOBNAME), jobDataMap);
        }
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    String jobName = jobKey.getName();
                    if (!map.containsKey(jobName)) {
                        clearScheduledJob(jobKey);
                    }
                    map.remove(jobName);
                }
            }
        } catch (SchedulerException schedulerException) {
            log.error("[RiskEngine][Schedule] scheduled jobs update failed: failed to get scheduler ", schedulerException);
        }
        for (Map.Entry<String, JobDataMap> entry: map.entrySet()) {
            schedule(entry.getValue());
        }
        log.info("[RiskEngine][Schedule] update scheduled jobs completed");
        return listScheduledJobs();
    }

    /**
     * reschedule all jobs
     * @return a map of job key and next job trigger time
     */
    public Map<String, String> rescheduleJobs() {
        List<JobDataMap> jobDataMaps = createJobDataMap();
        return updateScheduledJobs(jobDataMaps);
    }

    /**
     * create job data map from schedule Rules
     * @return a list of JobDataMap
     */
    public List<JobDataMap> createJobDataMap() {
        Map<String, BaseSchedulesConfig.ScheduleRule> scheduleRules = baseSchedulesConfig.parseScheduleRuleJson(baseSchedulesConfig.getSchedules());
        List<JobDataMap> jobDataMapList = new LinkedList<>();
        if (scheduleRules.size() == 0)
            return jobDataMapList;

        for (Map.Entry<String, BaseSchedulesConfig.ScheduleRule> scheduleEntry: scheduleRules.entrySet()) {
            BaseSchedulesConfig.ScheduleRule scheduleRuleObject = scheduleEntry.getValue();
            for (String cronSchedule: scheduleRuleObject.getCronSchedules()) {
                JobDataMap jobDataMap = new JobDataMap();
                jobDataMap.put(GROUPNAME, GROUPNAME);
                jobDataMap.put(JOBNAME, scheduleRuleObject.getKey() + "|" + cronSchedule);
                jobDataMap.put(SCHEDULERULE, scheduleRuleObject);
                jobDataMap.put(CRONSCHEDULE, cronSchedule);
                jobDataMapList.add(jobDataMap);
            }
        }
        return jobDataMapList;
    }
}
