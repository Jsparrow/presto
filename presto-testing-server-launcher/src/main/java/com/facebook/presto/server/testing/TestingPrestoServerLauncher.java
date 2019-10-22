/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.testing;

import com.facebook.presto.server.testing.TestingPrestoServerLauncherOptions.Catalog;
import com.facebook.presto.spi.Plugin;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.HelpOption;
import io.airlift.airline.model.CommandMetadata;

import javax.inject.Inject;

import static io.airlift.airline.SingleCommand.singleCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "testing_presto_server", description = "Testing Presto Server Launcher")
public class TestingPrestoServerLauncher
{
    private static final Logger logger = LoggerFactory.getLogger(TestingPrestoServerLauncher.class);

	@Inject
    CommandMetadata commandMetadata;

    @Inject
    public HelpOption helpOption;

    @Inject
    TestingPrestoServerLauncherOptions options = new TestingPrestoServerLauncherOptions();

    private static void waitForInterruption()
    {
        try {
            Thread.currentThread().join();
        }
        catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
			Thread.currentThread().interrupt();
        }
    }

    public void run()
            throws Exception
    {
        try (TestingPrestoServer server = new TestingPrestoServer()) {
            for (String pluginClass : options.getPluginClassNames()) {
                Plugin plugin = (Plugin) Class.forName(pluginClass).getConstructor().newInstance();
                server.installPlugin(plugin);
            }

            options.getCatalogs().forEach(catalog -> server.createCatalog(catalog.getCatalogName(), catalog.getConnectorName()));

            logger.info(String.valueOf(server.getAddress()));
            waitForInterruption();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingPrestoServerLauncher launcher = singleCommand(TestingPrestoServerLauncher.class).parse(args);
        if (launcher.helpOption.showHelpIfRequested()) {
            return;
        }
        try {
            launcher.validateOptions();
        }
        catch (IllegalStateException e) {
            logger.info("ERROR: " + e.getMessage(), e);
            System.out.println();
            Help.help(launcher.commandMetadata);
            return;
        }
        launcher.run();
    }

    private void validateOptions()
    {
        options.validate();
    }
}
