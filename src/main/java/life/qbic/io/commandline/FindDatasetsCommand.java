package life.qbic.io.commandline;

import ch.ethz.sis.openbis.generic.OpenBIS;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.DataSet;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.Experiment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.Person;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample;

import java.text.SimpleDateFormat;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import life.qbic.App;
import life.qbic.model.Configuration;
import life.qbic.model.download.FileSystemWriter;
import life.qbic.model.download.SummaryWriter;
import life.qbic.model.DatasetWithProperties;
import life.qbic.model.download.OpenbisConnector;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;



/**
 * List Data
 * The list-data command can be used to list Datasets in openBIS and some of their metadata based
 * on the experiment or sample they are attached to. Experiments or samples are specified by their
 * openBIS code or identifier.
 * The optional 'space' parameter can be used to only show datasets found in the provided space.
 * If no dataset name but a 'space' parameter is given, the command will list all datasets in the 
 * provided space.
 */
@Command(name = "list-data",
    description = "lists datasets and their details for a given experiment code")
public class FindDatasetsCommand implements Runnable {

  @Parameters(arity = "0..1", paramLabel = "openBIS object", description =
      "The code of the experiment or sample data is attached to.")
  private String objectCode;
  @Option(arity = "1", paramLabel = "<space>", description = "Optional openBIS spaces to filter "
      + "found datasets by.", names = {"-s", "--space"})
  private String space;
  @Mixin
  OpenbisAuthenticationOptions auth = new OpenbisAuthenticationOptions();

  @Override
    public void run() {
      App.readConfig();
      List<String> spaces = new ArrayList<>();
      if (space != null) {
        System.out.println("Querying experiment in space: " + space + "...");
        spaces.add(space);
      } else {
        System.out.println("Querying experiment in all available spaces...");
      }
      
      OpenBIS authentication = App.loginToOpenBIS(auth.getOpenbisPassword(), auth.getOpenbisUser(),
          auth.getOpenbisAS());
      OpenbisConnector openbis = new OpenbisConnector(authentication);

      List<DataSet> datasets = new ArrayList<>();

      if (spaces.isEmpty() && objectCode == null) {
        System.out.println("No space or object code provided."); //TODO
        return;
      }

      List<String> objectCodes = new ArrayList<>();
      if (!spaces.isEmpty() && objectCode == null) {
        Map<String, List<Experiment>> listExperimentsbySpace = openbis.getExperimentsBySpace(spaces);
        listExperimentsbySpace.forEach((space, experiments) -> {
          experiments.forEach(experiment -> {
            objectCodes.add(experiment.getIdentifier().toString());
          });
        });
        Map<String, List<Sample>> listSamplesbySpace = openbis.getSamplesBySpace(spaces);
        listSamplesbySpace.forEach((space, samples) -> {
          samples.forEach(sample -> {
            objectCodes.add(sample.getIdentifier().toString());
          });
        });
      } 
      if (objectCode != null) {
        objectCodes.add(objectCode);
      }

      List<String> objectCodesShortened = new ArrayList<>();
      for (String code : objectCodes) {
        if (code.contains("/")) {
          String[] splt = code.split("/");
          code = splt[splt.length - 1];
          System.out.println("Query is not an object code, querying for: " + code + " instead.");
        }
        objectCodesShortened.add(code);
      }

      List<String> summary = new ArrayList<>();

      for (String code : objectCodesShortened) {
        List<DataSet> datasetsOfExp = openbis.listDatasetsOfExperiment(spaces, code).stream()
        .sorted(Comparator.comparing(
            (DataSet d) -> d.getExperiment().getProject().getSpace().getCode()))
        .collect(Collectors.toList());

        if (!datasetsOfExp.isEmpty()) {
            summary.add(String.format("Found %s datasets for experiment %s:", datasetsOfExp.size(), code));
          datasets.addAll(datasetsOfExp);
          summary = getDetailsOfDataset(datasetsOfExp, openbis, summary);
        }

        List<DataSet> datasetsOfSample = openbis.listDatasetsOfSample(spaces, code).stream()
        .sorted(Comparator.comparing(
            (DataSet d) -> d.getExperiment().getProject().getSpace().getCode()))
        .collect(Collectors.toList());

        if (!datasetsOfSample.isEmpty()) {
          summary.add(String.format("Found %s datasets of sample %s:", datasetsOfSample.size(), code));
          datasets.addAll(datasetsOfSample);
          summary = getDetailsOfDataset(datasetsOfSample, openbis, summary);
        }
      }
      for(String s : summary) {
        System.out.println(s);
      }
      saveSummary(summary);      
  }

  /**
 * Get Details of Dataset
 * Gets the properties of the given dataset and adds them to the summary.
 */
  private List<String> getDetailsOfDataset(List<DataSet> datasets, OpenbisConnector openbis, List<String> summary) {
    Map<String, String> properties = new HashMap<>();
      if (!datasets.isEmpty()) {
        Set<String> patientIDs = openbis.findPropertiesInSampleHierarchy("PATIENT_DKFZ_ID",
        datasets.get(0).getExperiment().getIdentifier());
        if (!patientIDs.isEmpty()) {
          properties.put("patientIDs", String.join(",", patientIDs));
        }
      }

      List<DatasetWithProperties> datasetWithProperties = datasets.stream().map(dataSet -> {
        DatasetWithProperties ds = new DatasetWithProperties(dataSet);
        for (String key : properties.keySet()) {
          ds.addProperty(key, properties.get(key));
        }
        return ds;
      }).collect(Collectors.toList());

      int datasetIndex = 0;
      for (DatasetWithProperties dataSet : datasetWithProperties) {
        datasetIndex++;
        summary.add("[" + datasetIndex + "]");
        for (String key : dataSet.getProperties().keySet()) {
          summary.add(key + ": " + properties.get(key));
        }
        summary.add(String.format("ID: %s (%s)", dataSet.getCode(), dataSet.getExperiment().getIdentifier()));
        summary.add("Type: " + dataSet.getType().getCode());
        Person person = dataSet.getRegistrator();
        String simpleTime = new SimpleDateFormat("MM-dd-yy HH:mm:ss").format(dataSet.getRegistrationDate());
        String name = person.getFirstName() + " " + person.getLastName();
        String uploadedBy = "Uploaded by " + name + " (" + simpleTime + ")";
        summary.add(uploadedBy);
        summary.add("");
      }
      return summary;
  }

  /**
  * Saves the Summary to File
  * Saves the summary in the log-Folder as txt-file.
  */
  private void saveSummary(List<String> summary) {
    Path outputPath = Paths.get(Configuration.LOG_PATH.toString(), "find_datasets_summary" + getTimeStamp() + ".txt");
    SummaryWriter summaryWriter = new FileSystemWriter(outputPath);
    try {
      summaryWriter.reportSummary(summary);
    } catch (IOException e) {
      throw new RuntimeException("Could not write summary file.");
    }
  }

  private String getTimeStamp() {
    final String PATTERN_FORMAT = "YYYY-MM-dd_HHmmss";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(PATTERN_FORMAT);
    return LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).format(formatter);
  }
}