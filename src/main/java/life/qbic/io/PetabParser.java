package life.qbic.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import life.qbic.model.petab.PetabMetadata;

public class PetabParser {

  private final String META_INFO_YAML_NAME = "metaInformation";

  public PetabMetadata parse(String dataPath) {

    File directory = new File(dataPath);
    List<String> sourcePetabReferences = new ArrayList<>();

    File yaml = findYaml(directory);
    if (yaml != null) {
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(yaml));
        boolean inIDBlock = false;
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          // the id block ends, when a new key with colon is found
          if (inIDBlock && line.contains(":")) {
            inIDBlock = false;
          }
          // if we are in the id block, we collect one dataset code per line
          if (inIDBlock) {
            parseDatasetCode(line).ifPresent(sourcePetabReferences::add);
          }
          if (line.contains("openBISSourceIds:")) {
            inIDBlock = true;
          }
        }
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new PetabMetadata(sourcePetabReferences);
  }

  private Optional<String> parseDatasetCode(String line) {
    // expected input: "    - 20240702093837370-684137"
    String[] tokens = line.split("-");
    if(tokens.length == 3) {
      return Optional.of(tokens[1].strip()+"-"+tokens[2].strip());
    } else {
      System.out.println("Could not extract dataset code from the following line:");
      System.out.println(line);
    }
    return Optional.empty();
  }

  public void addDatasetId(String outputPath, String datasetCode) throws IOException {

    Path path = Paths.get(Objects.requireNonNull(findYaml(new File(outputPath))).getPath());
    Charset charset = StandardCharsets.UTF_8;

    final String idKeyWord = "openBISId";

    final String endOfLine = ":(.*)?(\\r\\n|[\\r\\n])";
    final String idInLine = idKeyWord+endOfLine;

    String content = Files.readString(path, charset);
    // existing property found, fill/replace with relevant dataset code
    if(content.contains(idKeyWord)) {
      content = content.replaceAll(idInLine, idKeyWord+": "+datasetCode+"\n");
      // no existing property found, create it above the dateOfExperiment property
    } else {
      String dateKeyword = "dateOfExperiment";
      String dateLine = dateKeyword+endOfLine;
      String newLines = idKeyWord+": "+datasetCode+"\n  "+dateKeyword+":\n";
      content = content.replaceAll(dateLine, newLines);
    }
    Files.write(path, content.getBytes(charset));
  }

  private File findYaml(File directory) {
    for (File file : Objects.requireNonNull(directory.listFiles())) {
      String fileName = file.getName();
      if (file.isFile() && fileName.contains(META_INFO_YAML_NAME) && fileName.endsWith(".yaml")) {
        return file;
      }
      if (file.isDirectory()) {
        return findYaml(file);
      }
    }
    System.out.println(META_INFO_YAML_NAME + " yaml not found.");
    return null;
  }

  public void addPatientIDs(String outputPath, Set<String> patientIDs) {
    System.err.println("found patient ids: "+patientIDs);
  }
}
