package com.github.bsideup.liiklus.positions.inmemory;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;
import org.springframework.context.ApplicationContext;

class InMemoryPositionsStorageTest implements PositionsStorageTests {

    static final ApplicationContext applicationContext = new ApplicationRunner("MEMORY", "MEMORY").run();

    @Getter
    PositionsStorage storage = applicationContext.getBean(PositionsStorage.class);

}