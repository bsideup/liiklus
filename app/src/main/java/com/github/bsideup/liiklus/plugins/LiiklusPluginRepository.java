package com.github.bsideup.liiklus.plugins;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.pf4j.PluginRepository;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class LiiklusPluginRepository implements PluginRepository {

    final LiiklusPluginManager liiklusPluginManager;

    @Override
    @SneakyThrows
    public List<Path> getPluginPaths() {
        PathMatcher[] matchers = Stream.of(liiklusPluginManager.getPluginsPathMatcher().split("/"))
                .map(it -> FileSystems.getDefault().getPathMatcher("glob:" + it))
                .toArray(PathMatcher[]::new);

        List<Path> result = new ArrayList<>();

        Files.walkFileTree(liiklusPluginManager.getPluginsRoot(), Collections.emptySet(), matchers.length, new FileVisitor<Path>() {

            int depth = -1;

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (depth == -1 || matchers[depth].matches(dir.getFileName())) {
                    depth++;
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (matchers[depth].matches(file.getFileName())) {
                    result.add(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                depth--;
                return FileVisitResult.CONTINUE;
            }
        });
        return result;
    }

    @Override
    public boolean deletePluginPath(Path pluginPath) {
        // TODO
        return false;
    }
}
