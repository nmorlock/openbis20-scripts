package life.qbic.io.commandline;

import ch.ethz.sis.openbis.generic.OpenBIS;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.SampleType;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.parsers.ParserConfigurationException;
import life.qbic.App;
import life.qbic.model.OpenbisSeekTranslator;
import life.qbic.model.SampleTypesAndMaterials;
import life.qbic.model.download.OpenbisConnector;
import life.qbic.model.download.SEEKConnector;
import org.apache.commons.codec.binary.Base64;
import org.xml.sax.SAXException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "sample-type-transfer",
    description =
        "Transfers sample types from openBIS to SEEK.")
public class TransferSampleTypesToSeekCommand implements Runnable {
  @Mixin
  SeekAuthenticationOptions seekAuth = new SeekAuthenticationOptions();
  @Mixin
  OpenbisAuthenticationOptions openbisAuth = new OpenbisAuthenticationOptions();
  @Option(names = "--ignore-existing", description = "Use to specify that existing "
      + "sample-types of the same name in SEEK should be ignored and the sample-type created a "
      + "second time.")
  private boolean ignoreExisting;
  @Option(names = "--sampletype-blacklist", description = "Path to file specifying by sampletype "
          + "sampletype code which openBIS sampletype not to transfer to SEEK. The file must contain one code "
          + "per line.")
  private String blacklistFile;
  OpenbisConnector openbis;
  SEEKConnector seek;
  OpenbisSeekTranslator translator;
  private Set<String> blacklistedSampleTypes = new HashSet<>(); // Set for blacklisted sample types

  @Override
  public void run() {
    App.readConfig();

    System.out.println("auth...");

    OpenBIS authentication = App.loginToOpenBIS(openbisAuth.getOpenbisPassword(),
        openbisAuth.getOpenbisUser(), openbisAuth.getOpenbisAS(), openbisAuth.getOpenbisDSS());
    System.out.println("openbis...");

    if(blacklistFile!=null && !blacklistFile.isBlank()) {
      parseBlackList(blacklistFile);
      System.out.printf("File with sampletype codes that won't be transferred: %s%n", blacklistFile);
    }

    openbis = new OpenbisConnector(authentication);

    byte[] httpCredentials = Base64.encodeBase64(
        (seekAuth.getSeekUser() + ":" + new String(seekAuth.getSeekPassword())).getBytes());
    try {
      String project = App.configProperties.get("seek_default_project");
      if(project == null || project.isBlank()) {
        throw new RuntimeException("a default project must be provided via config "+
            "('seek_default_project') or parameter.");
      }
      seek = new SEEKConnector(seekAuth.getSeekUser(), seekAuth.getSeekURL(), httpCredentials, openbisAuth.getOpenbisBaseURL(),
          App.configProperties.get("seek_default_project"));
      translator = seek.getTranslator();
    } catch (URISyntaxException | IOException | InterruptedException |
             ParserConfigurationException | SAXException e) {
      throw new RuntimeException(e);
    }

    SampleTypesAndMaterials types = openbis.getSampleTypesWithMaterials();

    try {
      for(SampleType type : types.getSampleTypes()) {
        String sampleTypeCode = type.getCode();
        if (blacklistedSampleTypes.contains(sampleTypeCode)) {
          System.out.println("Skipping blacklisted sample type: " + sampleTypeCode);
          continue;
        }
        System.err.println("creating " + sampleTypeCode);
        if(!ignoreExisting && seek.sampleTypeExists(sampleTypeCode)) {
          System.err.println(sampleTypeCode + " is already in SEEK. If you want to create a new "
                  + "sample type using the same name, you can use the --ignore-existing flag.");
        } else {
          String sampleTypeId = seek.createSampleType(translator.translate(type));
          System.out.println("Created SEEK sample_type Id "+sampleTypeId);
        }
      }
    } catch (URISyntaxException | IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    System.out.println("Done");
  }

  private void parseBlackList(String blacklistFile) {
    try (Stream<String> lines = Files.lines(Paths.get(blacklistFile))
            .map(String::trim)
            .filter(s -> !s.isBlank())) {
      Set<String> codes = lines.collect(Collectors.toSet());
      blacklistedSampleTypes.addAll(codes);
    } catch (IOException e) {
      throw new RuntimeException(blacklistFile+" could not be found or read.");
    }
  }

}
