package com.github.bsideup.liiklus.plugins;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.pf4j.PluginRepository;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class LiiklusPluginRepository implements PluginRepository {

    final LiiklusPluginManager liiklusPluginManager;

    @Override
    @SneakyThrows
    public List<Path> getPluginPaths() {
        val pluginsRoot = liiklusPluginManager.getPluginsRoot();
        val pathMatcher = liiklusPluginManager.getPluginsPathMatcher();

        val locationPattern = "file:" + pluginsRoot.resolve(pathMatcher).toString();
        return Stream.of(new PathMatchingResourcePatternResolver().getResources(locationPattern))
                .map(it -> {
                    try {
                        return it.getFile().toPath();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public boolean deletePluginPath(Path pluginPath) {
        // TODO
        return false;
    }
}
