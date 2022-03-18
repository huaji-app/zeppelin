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
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job.Status;
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

    sendEmailNotificationIfError(zeppelinConfiguration, note, result);

    if (sample != null) {
      Tag noteId = Tag.of("nodeid", note.getId());
      Tag name = Tag.of("name", StringUtils.defaultString(note.getName(), "unknown"));
      Tag statusTag = Tag.of("result", result);
      sample.stop(Metrics.timer("cronjob", Tags.of(noteId, name, statusTag)));
    } else {
      LOGGER.warn("No Timer.Sample for NoteId {} found", note.getId());
    }
  }

  /**
   * <pre>
   * When email is enabled, the following condition will trigger email notification:
   * 1. job result != RESULT_SUCCEEDED
   * 2. any paragraph's status = ERROR
   * </pre>
   */
  private void sendEmailNotificationIfError(ZeppelinConfiguration conf, Note note, String jobResult) {
    if (!conf.getBoolean(ConfVars.ZEPPELIN_EMAIL_ENABLE)) {
      return;
    }

    StringBuilder errorMessageBuilder = new StringBuilder();
    for (Paragraph paragraph : note.getParagraphs()) {
      if (paragraph.getStatus().equals(Status.ERROR)) {
        StringBuilder errorMessage = new StringBuilder();
        if (paragraph.getException() != null) {
          errorMessage.append(paragraph.getException()).append("; ");
        }
        if (paragraph.getErrorMessage() != null) {
          errorMessage.append(paragraph.getErrorMessage()).append("; ");
        }
        if (paragraph.getReturn() != null) {
          errorMessage.append(paragraph.getReturn()).append("; ");
        }
        errorMessageBuilder.append(String.format("Paragraph %s: %s",
            paragraph.getId(), errorMessage));
        errorMessageBuilder.append('\n');
      }
    }

    if (errorMessageBuilder.length() == 0 && jobResult.equals(CronJob.RESULT_SUCCEEDED)) {
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
    String subject = String.format("%s - Note %s has error", serverName, note.getName());
    String textContent = String.format("Server: %s\n"
            + "Note: %s\n"
            + "Job status: %s\n"
            + "Date: %s\n"
            + "Link: %s\n"
            + "---\nParagraph errors:\n%s\n",
        serverName,
        note.getName(),
        jobResult,
        new Date(),
        conf.getString(ConfVars.ZEPPELIN_SERVER_URL) + "#/notebook/" + note.getId(),
        errorMessageBuilder
    );

    Email email = EmailBuilder.startingBlank()
        .from(
            conf.getString(ConfVars.ZEPPELIN_EMAIL_DISPLAY_NAME),
            conf.getString(ConfVars.ZEPPELIN_EMAIL_FROM)
        )
        .withReplyTo(conf.getString(ConfVars.ZEPPELIN_EMAIL_REPLY_TO))
        .to(recipients)
        .withSubject(subject)
        .withPlainText(textContent)
        .buildEmail();
    LOGGER.info("Sending email to {}, subject: {}, content: {}", recipients, subject, textContent);
    mailer.sendMail(email);
  }
}
