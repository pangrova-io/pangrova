package org.pangrova.shuffle.deployment;

import java.nio.file.Path;

import org.pangrova.shuffle.server.ShuffleServerRuntime;

/** Process entry point for the canonical v2 durable shuffle server. */
public final class PangrovaShuffleServer {

    private PangrovaShuffleServer() {}

    /** Starts the server from one strict YAML file and blocks until shutdown. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "usage: PangrovaShuffleServer <configuration.yml>");
        }
        try (ShuffleServerRuntime runtime = ShuffleServerRuntime.start(
                ShuffleServiceConfig.load(Path.of(args[0])))) {
            Runtime.getRuntime().addShutdownHook(new Thread(runtime::close,
                    "pangrova-shuffle-shutdown"));
            runtime.awaitTermination();
        }
    }
}
