package com.github.bsideup.liiklus.positions.inmemory;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;

class InMemoryPositionsStorageTest implements PositionsStorageTests {

    @Getter
    PositionsStorage storage = new InMemoryPositionsStorage();

}