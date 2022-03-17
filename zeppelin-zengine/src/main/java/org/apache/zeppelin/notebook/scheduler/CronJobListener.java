package org.apache.zeppelin.notebook.scheduler;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.mail.Message.RecipientType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.notebook.Note;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.email.Recipient;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronJobListener implements JobListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(CronJobListener.class);

  // JobExecutionContext -> Timer.Sample
  private Map<JobExecutionContext, Timer.Sample> cronJobTimerSamples = new HashMap<>();

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public void jobToBeExecuted(JobExecutionContext context) {
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    Note note = (Note) jobDataMap.get("note");
    LOGGER.info("Start cron job of note: {}", note.getId());
    cronJobTimerSamples.put(context, Timer.start(Metrics.globalRegistry));
  }

  @Override
  public void jobExecutionVetoed(JobExecutionContext context) {
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    Note note = (Note) jobDataMap.get("note");
    LOGGER.info("vetoed cron job of note: {}", note.getId());
  }

  @Override
  public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    Note note = (Note) jobDataMap.get("note");
    ZeppelinConfiguration zeppelinConfiguration = (ZeppelinConfiguration) jobDataMap.get("zeppelinConfiguration");
    String result = StringUtils.defaultString(context.getResult().toString(), "unknown");
    LOGGER.info("cron job of noteId {} executed with result {}", note.getId(), result);
    Timer.Sample sample = cronJobTimerSamples.remove(context);
    if (!result.equals(CronJob.RESULT_SUCCEEDED)) {
        sendEmailNotificationWhenFail(zeppelinConfiguration, note, result);
      }
      if (sample != null) {
      Tag noteId = Tag.of("nodeid", note.getId());
      Tag name = Tag.of("name", StringUtils.defaultString(note.getName(), "unknown"));
      Tag statusTag = Tag.of("result", result);
      sample.stop(Metrics.timer("cronjob", Tags.of(noteId, name, statusTag)));
    } else {
      LOGGER.warn("No Timer.Sample for NoteId {} found", note.getId());
    }
  }

  private void sendEmailNotificationWhenFail(ZeppelinConfiguration conf, Note note, String jobResult) {
    if (!conf.getBoolean(ConfVars.ZEPPELIN_EMAIL_ENABLE)) {
      return;
    }
    Mailer mailer = MailerBuilder
        .withSMTPServer(conf.getString(ConfVars.ZEPPELIN_EMAIL_SMTP_ADDRESS),
            conf.getInt(ConfVars.ZEPPELIN_EMAIL_SMTP_PORT),
            conf.getString(ConfVars.ZEPPELIN_EMAIL_SMTP_USER_NAME),
            conf.getString(ConfVars.ZEPPELIN_EMAIL_SMTP_PASSWORD))
        .withTransportStrategy(TransportStrategy.SMTP)
        .withSessionTimeout(10 * 1000)
        .withProperty("mail.smtp.sendpartial", true)
        .withDebugLogging(true)
        .buildMailer();

    String[] recipientsArr = conf.getString(ConfVars.ZEPPELIN_EMAIL_RECIPIENTS).split(",");
    List<Recipient> recipients = new ArrayList<>();
    for (String recipient : recipientsArr) {
      recipients.add(new Recipient(recipient, recipient, RecipientType.TO));
    }

    String serverName = conf.getString(ConfVars.ZEPPELIN_SERVER_NAME);
    String textContent = String.format("Server: %s"
            + "Note: %s"
            + "Date: %s"
            + "Link: %s",
        serverName,
        note.getName(),
        new Date(),
        conf.getString(ConfVars.ZEPPELIN_SERVER_URL) + "#/notebook/" + note.getId()
        );

    Email email = EmailBuilder.startingBlank()
        .from(
            conf.getString(ConfVars.ZEPPELIN_EMAIL_DISPLAY_NAME),
            conf.getString(ConfVars.ZEPPELIN_EMAIL_FROM)
        )
        .withReplyTo(conf.getString(ConfVars.ZEPPELIN_EMAIL_REPLY_TO))
        .to(recipients)
        .withSubject(String.format("%s - Job Result = %s for %s", serverName, jobResult, note.getName()))
        .withPlainText(textContent)
        .buildEmail();
    mailer.sendMail(email);
  }
}
