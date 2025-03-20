package io.github.chedwick.sqlcompat;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.DirectoryScanner;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.xml.sax.InputSource;

/**
 * Scans mapper XML files and extracts mapped statements.
 */
public final class MapperScanner {
    private static final Pattern IF_TEST_TOKEN = Pattern.compile("([A-Za-z_][\\w\\.]+)");
    private static final Set<String> IF_TEST_KEYWORDS = Set.of("null", "and", "or", "not", "true", "false", "empty");

    private final org.apache.maven.plugin.logging.Log log;
    private final List<String> directories;
    private final List<String> includes;
    private final List<String> excludes;

    public MapperScanner(org.apache.maven.plugin.logging.Log log,
                         List<String> directories,
                         List<String> includes,
                         List<String> excludes) {
        this.log = log;
        this.directories = directories == null ? Collections.emptyList() : directories;
        this.includes = includes == null ? Collections.emptyList() : includes;
        this.excludes = excludes == null ? Collections.emptyList() : excludes;
    }

    public List<SqlStatement> scan() throws IOException {
        Map<String, SqlStatement> unique = new LinkedHashMap<>();
        for (String dir : directories) {
            
            Path base = Path.of(dir);
            if (!Files.exists(base)) {
                log.warn("Mapper directory does not exist: " + base);
                continue;
            }

            log.info("Scanning mapper directory: " + base.toAbsolutePath());
            DirectoryScanner scanner = new DirectoryScanner();
            scanner.setBasedir(base.toFile());
            String[] includePatterns = includes.isEmpty() ? new String[] {"**/*.xml"} : includes.toArray(new String[0]);
            scanner.setIncludes(includePatterns);
            scanner.setExcludes(excludes.toArray(new String[0]));
            if (includes.isEmpty()) {
                log.info("Include patterns: <none provided> (defaulting to **/*.xml)");
            } else {
                log.info("Include patterns: " + includes);
            }
            log.info("Exclude patterns: " + excludes);
            scanner.addDefaultExcludes();
            scanner.scan();
            String[] included = scanner.getIncludedFiles();
            log.info("Included files (" + included.length + "): " + String.join(", ", included));
            for (String rel : included) {
                Path file = base.resolve(rel);
                log.info(" - found mapper: " + file.toUri() + " (name: " + file.getFileName() + ")");
                for (SqlStatement stmt : parseFile(file)) {
                    String key = stmt.fullId() + "|" + stmt.kind() + "|" + file.toAbsolutePath();
                    if (unique.putIfAbsent(key, stmt) != null) {
                        log.info("Skipping duplicate mapped statement: " + key);
                    }
                }
            }
        }
        return new ArrayList<>(unique.values());
    }

    private List<SqlStatement> parseFile(Path file) throws IOException {
        try {
            DocumentBuilderFactory factory = newDocumentBuilderFactory();
            DocumentBuilder builder = factory.newDocumentBuilder();
            // Ignore external DTD fetches but allow DOCTYPE declarations common in MyBatis XML.
            builder.setEntityResolver((publicId, systemId) -> new InputSource(new StringReader("")));
            Document doc = builder.parse(file.toFile());
            Element root = doc.getDocumentElement();
            String namespace = root.getAttribute("namespace");

            Map<String, Set<String>> ifParamHints = collectIfParamHints(root, namespace);

            Configuration configuration = new Configuration();
            try (InputStream is = Files.newInputStream(file)) {
                XMLMapperBuilder mapperBuilder = new XMLMapperBuilder(is, configuration, file.toString(), configuration.getSqlFragments());
                mapperBuilder.parse();
            }

            List<SqlStatement> collected = new ArrayList<>();
            for (MappedStatement ms : configuration.getMappedStatements()) {
                if (ms.getId().contains("!selectKey")) {
                    continue;
                }
                SqlStatement.Kind kind = toKind(ms.getSqlCommandType());
                if (kind == SqlStatement.Kind.UNKNOWN) {
                    continue;
                }
                BoundSql boundSql = ms.getBoundSql(buildSampleParams(ifParamHints.getOrDefault(ms.getId(), Collections.emptySet())));
                List<ParameterSpec> params = toParameterSpecs(boundSql.getParameterMappings());
                String fullId = ms.getId();
                String ns = extractNamespace(fullId);
                String id = extractId(fullId);
                collected.add(new SqlStatement(id, ns.isBlank() ? namespace : ns, kind, file, normalizeWhitespace(boundSql.getSql()), params));
            }
            return collected;
        } catch (Exception e) {
            throw new IOException("Failed to parse mapper file " + file, e);
        }
    }

    private static String normalizeWhitespace(String sql) {
        if (sql == null) {
            return "";
        }
        String collapsed = sql.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        return collapsed.replaceAll(" +", " ").trim();
    }

    private static Map<String, Set<String>> collectIfParamHints(Element root, String namespace) {
        Map<String, Set<String>> hints = new HashMap<>();
        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            String tagName = node.getNodeName();
            if (SqlStatement.Kind.fromTagName(tagName) == SqlStatement.Kind.UNKNOWN) {
                continue;
            }
            Element element = (Element) node;
            String id = element.getAttribute("id");
            if (StringUtils.isBlank(id)) {
                id = "<unnamed>";
            }
            String fullId = namespace == null || namespace.isBlank() ? id : namespace + "." + id;
            Set<String> params = new HashSet<>();
            collectIfTests(element, params);
            hints.put(fullId, params);
        }
        return hints;
    }

    private static void collectIfTests(Node node, Set<String> collector) {
        if (node.getNodeType() != Node.ELEMENT_NODE) {
            return;
        }
        Element el = (Element) node;
        if ("if".equalsIgnoreCase(el.getNodeName())) {
            String test = el.getAttribute("test");
            if (StringUtils.isNotBlank(test)) {
                Matcher matcher = IF_TEST_TOKEN.matcher(test);
                while (matcher.find()) {
                    String token = matcher.group(1);
                    if (!IF_TEST_KEYWORDS.contains(token.toLowerCase(Locale.ROOT))) {
                        collector.add(token);
                    }
                }
            }
        }
        NodeList children = el.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            collectIfTests(children.item(i), collector);
        }
    }

    private static Map<String, Object> buildSampleParams(Set<String> names) {
        DefaultParamMap root = new DefaultParamMap();
        for (String name : names) {
            if (StringUtils.isBlank(name)) {
                continue;
            }
            String[] parts = name.split("\\.");
            DefaultParamMap current = root;
            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                if (i == parts.length - 1) {
                    current.putIfAbsent(part, DefaultParamMap.SAMPLE_VALUE);
                } else {
                    Object next = current.get(part);
                    if (!(next instanceof DefaultParamMap)) {
                        next = new DefaultParamMap();
                        current.put(part, next);
                    }
                    current = (DefaultParamMap) next;
                }
            }
        }
        return root;
    }

    private static List<ParameterSpec> toParameterSpecs(List<ParameterMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return Collections.emptyList();
        }
        List<ParameterSpec> specs = new ArrayList<>(mappings.size());
        for (ParameterMapping mapping : mappings) {
            String jdbcType = mapping.getJdbcType() == null ? null : mapping.getJdbcType().name();
            specs.add(new ParameterSpec(mapping.getProperty(), jdbcType));
        }
        return specs;
    }

    private static String extractNamespace(String fullId) {
        if (fullId == null) {
            return "";
        }
        int dot = fullId.lastIndexOf('.');
        return dot > 0 ? fullId.substring(0, dot) : "";
    }

    private static String extractId(String fullId) {
        if (fullId == null) {
            return "<unnamed>";
        }
        int dot = fullId.lastIndexOf('.');
        return dot > 0 ? fullId.substring(dot + 1) : fullId;
    }

    private static SqlStatement.Kind toKind(SqlCommandType commandType) {
        if (commandType == null) {
            return SqlStatement.Kind.UNKNOWN;
        }
        return switch (commandType) {
            case SELECT -> SqlStatement.Kind.SELECT;
            case INSERT -> SqlStatement.Kind.INSERT;
            case UPDATE -> SqlStatement.Kind.UPDATE;
            case DELETE -> SqlStatement.Kind.DELETE;
            default -> SqlStatement.Kind.UNKNOWN;
        };
    }

    private static final class DefaultParamMap extends HashMap<String, Object> {
        private static final Object SAMPLE_VALUE = Integer.valueOf(1);

        @Override
        public Object get(Object key) {
            Object val = super.get(key);
            if (val == null) {
                val = SAMPLE_VALUE;
                super.put(String.valueOf(key), val);
            }
            return val;
        }

        @Override
        public int size() {
            return Math.max(1, super.size());
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

    private static DocumentBuilderFactory newDocumentBuilderFactory() throws ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setExpandEntityReferences(false);
        factory.setValidating(false);
        // Prevent external entity/DTD loading which can hang or be blocked in restricted environments.
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        return factory;
    }
}
