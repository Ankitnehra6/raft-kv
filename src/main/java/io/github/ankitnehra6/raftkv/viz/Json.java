package io.github.ankitnehra6.raftkv.viz;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A minimal JSON writer.
 *
 * <p>Hand-rolled rather than pulling in Jackson. The visualiser emits one small, fixed
 * schema, and this project's whole argument is that a consensus implementation should be
 * readable without a dependency tree behind it — adding one so a demo page can render would
 * undercut that for no benefit.
 */
final class Json {

    private Json() {}

    static String object(Map<String, Object> fields) {
        StringJoiner joiner = new StringJoiner(",", "{", "}");
        fields.forEach((key, value) -> joiner.add(quote(key) + ":" + value(value)));
        return joiner.toString();
    }

    static String array(List<?> items) {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        items.forEach(item -> joiner.add(value(item)));
        return joiner.toString();
    }

    @SuppressWarnings("unchecked")
    private static String value(Object value) {
        return switch (value) {
            case null -> "null";
            case String s -> quote(s);
            case Boolean b -> b.toString();
            case Number n -> n.toString();
            case Map<?, ?> m -> object((Map<String, Object>) m);
            case List<?> l -> array(l);
            case Enum<?> e -> quote(e.name());
            default -> quote(String.valueOf(value));
        };
    }

    /** Escapes per RFC 8259, including the control characters a naive writer forgets. */
    static String quote(String raw) {
        StringBuilder sb = new StringBuilder(raw.length() + 2).append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** Convenience for building a field map in declaration order. */
    static Map<String, Object> map() {
        return new java.util.LinkedHashMap<>();
    }

    static List<Object> list() {
        return new ArrayList<>();
    }
}
