package life.qbic.io.commandline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

// main command with format specifiers for the usage help message
@Command(name = "openbis-scripts",
    subcommands = {SampleHierarchyCommand.class, TransferSampleTypesToSeekCommand.class,
        DownloadPetabCommand.class, UploadPetabResultCommand.class, UploadDatasetCommand.class,
        SpaceStatisticsCommand.class, TransferDataToSeekCommand.class, FindDatasetsCommand.class,
        CreateROCrate.class},
    description = "A client software for querying openBIS.",
    mixinStandardHelpOptions = true, versionProvider = ManifestVersionProvider.class)
public class CommandLineOptions {

  private static final Logger LOG = LogManager.getLogger(CommandLineOptions.class);

  @Option(names = {"-config", "--config_file"},
      description = "Config file path to provide server and user information.",
      scope = CommandLine.ScopeType.INHERIT)
  static String configPath;

  @Option(names = {"-V", "--version"},
      versionHelp = true,
      description = "print version information",
      scope = CommandLine.ScopeType.INHERIT)
  boolean versionRequested = false;

  @Option(names = {"-h", "--help"},
      usageHelp = true,
      description = "display a help message and exit",
      scope = CommandLine.ScopeType.INHERIT)
  public boolean helpRequested = false;

  public static String getConfigPath() {
    return configPath;
  }
}
