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
import java.util.concurrent.*;

/**
 * Created by devrandom on 2015-Aug-30.
 */
public class StratumCli {
    public static final int CALL_TIMEOUT = 5000;
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
                .command(new HeaderCommand())
                .command(new HistoryCommand())
                .command(new BalanceCommand())
                .command(new GetTransactionCommand())
                .command(new UnspentCommand())
                .command(new VersionCommand())
                .command(new BannerCommand())
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
                            cleanup();
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
            stop(commandInvocation);
            return CommandResult.SUCCESS;
        }
    }

    private void stop(CommandInvocation commandInvocation) {
        commandInvocation.stop();
        cleanup();
    }

    private void cleanup() {
        client.stopAsync();
    }

    @CommandDefinition(name="version", description = "get server version")
    public class VersionCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            simpleCall("server.version");
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="banner", description = "get server banner")
    public class BannerCommand implements Command {
        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            simpleCall("server.banner");
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="history", description = "get address history")
    public class HistoryCommand implements Command {
        @Arguments(description = "addresses")
        List<String> addresses;

        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(addresses);
            ListenableFuture<StratumMessage> future = client.call("blockchain.address.get_history", params);
            try {
                StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
                print("result: ");
                println(formatResult(result));
            } catch (InterruptedException | ExecutionException e) {
                printerr("failed %s\n", e);
            } catch (TimeoutException e) {
                printerrln("timeout");
            }
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="balance", description = "get address balance")
    public class BalanceCommand implements Command {
        @Arguments(description = "addresses")
        List<String> addresses;

        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(addresses);
            ListenableFuture<StratumMessage> future = client.call("blockchain.address.get_balance", params);
            try {
                StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
                print("result: ");
                println(formatResult(result));
            } catch (InterruptedException | ExecutionException e) {
                printerr("failed %s\n", e);
            } catch (TimeoutException e) {
                printerrln("timeout");
            }
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="unspent", description = "list address unspent")
    public class UnspentCommand implements Command {
        @Arguments(description = "addresses")
        List<String> addresses;

        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(addresses);
            ListenableFuture<StratumMessage> future = client.call("blockchain.address.listunspent", params);
            try {
                StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
                print("result: ");
                println(formatResult(result));
            } catch (InterruptedException | ExecutionException e) {
                printerr("failed %s\n", e);
            } catch (TimeoutException e) {
                printerrln("timeout");
            }
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="header", description = "get block header")
    public class HeaderCommand implements Command {
        @Arguments(description = "hashes")
        List<String> hashes;

        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(hashes);
            ListenableFuture<StratumMessage> future = client.call("blockchain.block.get_header", params);
            try {
                StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
                print("result: ");
                println(formatResult(result));
            } catch (InterruptedException | ExecutionException e) {
                printerr("failed %s\n", e);
            } catch (TimeoutException e) {
                printerrln("timeout");
            }
            return CommandResult.SUCCESS;
        }
    }

    @CommandDefinition(name="transaction", description = "get transaction")
    public class GetTransactionCommand implements Command {
        @Arguments(description = "hashes")
        List<String> hashes;

        @Override
        public CommandResult execute(CommandInvocation commandInvocation) throws IOException, InterruptedException {
            List<Object> params = Lists.newArrayList();
            params.addAll(hashes);
            ListenableFuture<StratumMessage> future = client.call("blockchain.transaction.get", params);
            try {
                StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
                print("result: ");
                Object result1 = result.result;
                println(result1);
            } catch (InterruptedException | ExecutionException e) {
                printerr("failed %s\n", e);
            } catch (TimeoutException e) {
                printerrln("timeout");
            }
            return CommandResult.SUCCESS;
        }
    }

    private void println(Object item) {
        console.getShell().out().println(item);
    }

    private void printerrln(Object item) {
        console.getShell().err().println(item);
    }

    private void printerr(String format, Throwable ex) {
        console.getShell().err().printf(format, ex.getCause().getMessage());
    }

    private void print(Object item) {
        console.getShell().out().print(item);
    }

    private void simpleCall(String method) throws IOException {
        ListenableFuture<StratumMessage> future = client.call(method, Lists.newArrayList());
        try {
            StratumMessage result = future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
            print("result: ");
            println(result.result);
        } catch (InterruptedException | ExecutionException e) {
            printerr("failed %s\n", e);
        } catch (TimeoutException e) {
            printerrln("timeout");
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
                println(cmd);
                print(console.getHelpInfo(cmd));
                println("------");
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
                                if (item.isSentinel()) {
                                    headersChangeService.shutdown();
                                    break;
                                }
                                println(mapper.writeValueAsString(item));
                            } catch (InterruptedException | JsonProcessingException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                });
            }
            handleSubscriptionResult(subscription);
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
                                if (item.isSentinel()) {
                                    addressChangeService.shutdown();
                                    break;
                                }
                                println(mapper.writeValueAsString(item));
                            } catch (InterruptedException | JsonProcessingException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                });
            }
            handleSubscriptionResult(subscription);
            return CommandResult.SUCCESS;
        }
    }

    private void handleSubscriptionResult(StratumSubscription subscription) {
        Futures.addCallback(subscription.future, new FutureCallback<StratumMessage>() {
            @Override
            public void onSuccess(StratumMessage result) {
            }

            @Override
            public void onFailure(Throwable t) {
                printerr("failed %s\n", t);
            }
        });
        try {
            StratumMessage result = subscription.future.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
            print("initial state: ");
            println(formatResult(result));
        } catch (InterruptedException | ExecutionException e) {
            // ignore, handled by callback
        } catch (TimeoutException e) {
            printerrln("timeout");
        }
    }
}
