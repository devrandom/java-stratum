package org.smartwallet.stratum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jboss.aesh.cl.Arguments;
import org.jboss.aesh.cl.CommandDefinition;
import org.jboss.aesh.console.AeshConsole;
import org.jboss.aesh.console.AeshConsoleBuilder;
import org.jboss.aesh.console.Console;
import org.jboss.aesh.console.Prompt;
import org.jboss.aesh.console.command.Command;
import org.jboss.aesh.console.command.CommandResult;
import org.jboss.aesh.console.command.invocation.CommandInvocation;
import org.jboss.aesh.console.command.registry.AeshCommandRegistryBuilder;
import org.jboss.aesh.console.command.registry.CommandRegistry;
import org.jboss.aesh.console.helper.InterruptHook;
import org.jboss.aesh.console.settings.Settings;
import org.jboss.aesh.console.settings.SettingsBuilder;
import org.jboss.aesh.edit.actions.Action;
import org.jboss.aesh.terminal.Color;
import org.jboss.aesh.terminal.TerminalColor;
import org.jboss.aesh.terminal.TerminalString;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by devrandom on 2015-Aug-30.
 */
public class StratumCli {
    private StratumClient client;
    private AeshConsole console;
    private ObjectMapper mapper;
    private BlockingQueue<StratumMessage> addressChangeQueue;
    private ExecutorService addressChangeService;
    private BlockingQueue<StratumMessage> headersChangeQueue;
    private ExecutorService headersChangeService;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: StratumCli HOST:PORT");
            System.exit(1);
        }
        String[] hostPort = args[0].split(":");
        if (hostPort.length != 2) {
            System.err.println("Usage: StratumCli HOST:PORT");
            System.exit(1);
        }
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        new StratumCli().run(host, port);
    }

    private void run(String host, int port) {
        mapper = new ObjectMapper();
        client = new StratumClient(new InetSocketAddress(host, port), true);
        client.startAsync();
        CommandRegistry registry = new AeshCommandRegistryBuilder()
                .command(new ExitCommand())
                .command(new VersionCommand())
                .command(new HelpCommand())
                .command(new SubscribeAddressCommand())
                .command(new SubscribeHeadersCommand())
                .create();
        Settings settings = new SettingsBuilder()
                .logging(true)
                .persistHistory(true)
                .historyFile(new File(System.getProperty("user.home"), ".cache/stratum-cli.hist"))
                .interruptHook(new InterruptHook() {
                    @Override
                    public void handleInterrupt(Console console, Action action) {
                        if (action == Action.EOF) {
                            console.stop();
                        }
                    }
                })
                .create();
        console = new AeshConsoleBuilder()
                .commandRegistry(registry)
                .settings(settings)
                .prompt(new Prompt(new TerminalString("[aesh@rules]$ ",
                        new TerminalColor(Color.GREEN, Color.DEFAULT, Color.Intensity.BRIGHT))))
                .create();

        console.start();
    }

    @CommandDefinition(name="exit", description = "exit the program")
    public class ExitCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            commandInvocation.stop();
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="version", description = "get server version")
    public class VersionCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            ListenableFuture<StratumMessage> future = client.call("server.version", Lists.newArrayList());
            Futures.addCallback(future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage result) {
                    System.out.print("result: ");
                    System.out.println(formatResult(result));
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("failed.");
                }
            });
            return CommandResult.SUCCESS;
        }
    }

    private String formatResult(StratumMessage result) {
        try {
            return mapper.writeValueAsString(result.result);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @CommandDefinition(name="help", description = "show this message")
    public class HelpCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            for (String cmd : console.getCommandRegistry().getAllCommandNames()) {
                System.out.println(cmd);
                System.out.print(console.getHelpInfo(cmd));
                System.out.println("------");
            }
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="subscribe_headers", description = "subscribe to headers")
    public class SubscribeHeadersCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            StratumSubscription subscription = client.subscribe("blockchain.headers.subscribe", Lists.newArrayList());
            headersChangeQueue = subscription.queue;
            if (headersChangeService == null) {
                headersChangeService = Executors.newSingleThreadExecutor();
                headersChangeService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                StratumMessage item = headersChangeQueue.take();
                                if (item.isSentinel())
                                    break;
                                System.out.println(mapper.writeValueAsString(item));
                            } catch (InterruptedException | JsonProcessingException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                });
            }
            Futures.addCallback(subscription.future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage result) {
                    System.out.print("initial headers state: ");
                    System.out.println(formatResult(result));
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("failed.");
                }
            });
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="subscribe_address", description = "subscribe to headers")
    public class SubscribeAddressCommand implements Command {
        @Arguments(description = "addresses")
        List<String> addresses;
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(addresses);
            StratumSubscription subscription = client.subscribe("blockchain.address.subscribe", params);
            addressChangeQueue = subscription.queue;
            if (addressChangeService == null) {
                addressChangeService = Executors.newSingleThreadExecutor();
                addressChangeService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                StratumMessage item = addressChangeQueue.take();
                                if (item.isSentinel())
                                    break;
                                System.out.println(mapper.writeValueAsString(item));
                            } catch (InterruptedException | JsonProcessingException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                });
            }
            Futures.addCallback(subscription.future, new FutureCallback<StratumMessage>() {
                @Override
                public void onSuccess(StratumMessage result) {
                    System.out.print("initial address state: ");
                    System.out.println(formatResult(result));
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("failed.");
                }
            });
            return CommandResult.SUCCESS;
        }
    }
}
