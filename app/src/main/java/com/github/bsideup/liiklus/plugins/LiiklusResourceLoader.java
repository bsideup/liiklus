package com.github.bsideup.liiklus.plugins;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.pf4j.PluginManager;
import org.springframework.core.io.DefaultResourceLoader;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class LiiklusResourceLoader extends DefaultResourceLoader {

    PluginManager pluginManager;

    @Override
    public ClassLoader getClassLoader() {
        return new ClassLoader(Thread.currentThread().getContextClassLoader()) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                try {
                    return super.findClass(name);
                } catch (ClassNotFoundException e) {
                    // FIXME X_X
                    for (val pluginWrapper : pluginManager.getResolvedPlugins()) {
                        try {
                            return pluginWrapper.getPluginClassLoader().loadClass(name);
                        } catch (ClassNotFoundException __) {
                            continue;
                        }
                    }

                    throw e;
                }
            }
        };
    }
}
