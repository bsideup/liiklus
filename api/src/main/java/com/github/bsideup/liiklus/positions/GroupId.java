package com.github.bsideup.liiklus.positions;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.Comparator;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class GroupId implements Comparable<GroupId> {

    public static final Comparator<GroupId> COMPARATOR = Comparator
            .comparing(GroupId::getName)
            .thenComparing(it -> it.getVersion().orElse(0));

    public static final String VERSION_SEPARATOR = "-v";
    public static final Pattern VERSION_PATTERN = Pattern.compile("^(.*)-v(\\d+)$");

    public static GroupId of(String name, int version) {
        return of(name, Optional.of(version));
    }

    public static GroupId of(String name, Optional<Integer> version) {
        if (version.orElse(0) < 0) {
            throw new IllegalArgumentException("version must be >= 0");
        }
        return new GroupId(name, version.filter(it -> it != 0));
    }

    public static GroupId ofString(String str) {
        Matcher matcher = VERSION_PATTERN.matcher(str);

        if (matcher.matches()) {
            MatchResult result = matcher.toMatchResult();

            return GroupId.of(
                    result.group(1),
                    Optional.ofNullable(result.group(2)).map(Integer::parseInt)
            );
        } else {
            return GroupId.of(
                    str,
                    Optional.empty()
            );
        }
    }

    @NonNull
    String name;

    @NonNull
    Optional<Integer> version;

    public String asString() {
        return name + version.map(it -> VERSION_SEPARATOR + it).orElse("");
    }

    @Override
    public int compareTo(GroupId other) {
        return COMPARATOR.compare(this, other);
    }
}
