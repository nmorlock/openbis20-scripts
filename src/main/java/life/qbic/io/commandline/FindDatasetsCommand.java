package life.qbic.io.commandline;

import ch.ethz.sis.openbis.generic.OpenBIS;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.DataSet;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.Experiment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.Person;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import life.qbic.App;
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

      for (String code : objectCodesShortened) {
        List<DataSet> datasetsOfExp = openbis.listDatasetsOfExperiment(spaces, code).stream()
        .sorted(Comparator.comparing(
            (DataSet d) -> d.getExperiment().getProject().getSpace().getCode()))
        .collect(Collectors.toList());

        if (!datasetsOfExp.isEmpty()) {
          System.out.printf("Found %s datasets for experiment %s:%n", datasetsOfExp.size(), code);
          datasets.addAll(datasetsOfExp);
          printDataset(datasetsOfExp, openbis);
        }

        List<DataSet> datasetsOfSample = openbis.listDatasetsOfSample(spaces, code).stream()
        .sorted(Comparator.comparing(
            (DataSet d) -> d.getExperiment().getProject().getSpace().getCode()))
        .collect(Collectors.toList());

        if (!datasetsOfSample.isEmpty()) {
          System.out.printf("Found %s datasets for sample %s:%n", datasetsOfSample.size(), code);
          datasets.addAll(datasetsOfSample);
          printDataset(datasetsOfSample, openbis);
        }
      }
  }

  /**
 * Print Dataset
 * Gets the properties of the given dataset and prints them.
 */
  private void printDataset(List<DataSet> datasets, OpenbisConnector openbis) {
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
        System.out.println("[" + datasetIndex + "]");
        for (String key : dataSet.getProperties().keySet()) {
          System.out.println(key + ": " + properties.get(key));
        }
        System.out.printf("ID: %s (%s)%n", dataSet.getCode(), dataSet.getExperiment().getIdentifier());
        System.out.println("Type: " + dataSet.getType().getCode());
        Person person = dataSet.getRegistrator();
        String simpleTime = new SimpleDateFormat("MM-dd-yy HH:mm:ss").format(dataSet.getRegistrationDate());
        String name = person.getFirstName() + " " + person.getLastName();
        String uploadedBy = "Uploaded by " + name + " (" + simpleTime + ")";
        System.out.println(uploadedBy);
        System.out.println();
      }
  }
}