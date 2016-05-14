package org.vertexium.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import jline.TerminalFactory;
import jline.UnixTerminal;
import jline.UnsupportedTerminal;
import jline.WindowsTerminal;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.tools.shell.AnsiDetector;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.codehaus.groovy.tools.shell.Interpreter;
import org.codehaus.groovy.tools.shell.util.Logger;
import org.codehaus.groovy.tools.shell.util.NoExitSecurityManager;
import org.codehaus.groovy.tools.shell.util.Preferences;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.vertexium.Graph;
import org.vertexium.GraphFactory;
import org.vertexium.Visibility;
import org.vertexium.cli.commands.*;
import org.vertexium.util.ConfigurationUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VertexiumShell {
    private Groovysh groovysh;

    @Parameter(names = {"--help", "-h"}, description = "Print help", help = true)
    private boolean help;

    @Parameter(names = {"-C"}, description = "Suppress colors")
    private boolean suppressColor;

    @Parameter(names = {"-T"}, description = "Terminal type")
    private String terminalType = TerminalFactory.AUTO;

    @Parameter(names = {"-e"}, description = "String to evaluate")
    private String evalString = null;

    @Parameter(names = {"-c"}, description = "Configuration file name", required = true)
    private List<String> configFileNames = new ArrayList<>();

    @Parameter(names = {"-cp"}, description = "Configuration property prefix")
    private String configPropertyPrefix = null;

    @Parameter(names = {"-a"}, description = "Authorizations")
    private String authorizations = null;

    @Parameter(names = {"-t"}, description = "Time")
    private Long time = null;

    @Parameter(description = "File names to execute")
    private List<String> fileNames = new ArrayList<>();

    public int run(String[] args) throws Exception {
        JCommander j = new JCommander(this, args);
        if (help) {
            j.usage();
            return -1;
        }

        setTerminalType(terminalType, suppressColor);

        Map<String, String> config = ConfigurationUtils.loadConfig(configFileNames, configPropertyPrefix);
        Graph graph = new GraphFactory().createGraph(config);

        System.setProperty("groovysh.prompt", "vertexium");

        // IO must be constructed AFTER calling setTerminalType()/AnsiConsole.systemInstall(),
        // else wrapped System.out does not work on Windows.
        final IO io = new IO();

        Logger.io = io;

        CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
        VertexiumScript.setGraph(graph);
        if (authorizations != null) {
            VertexiumScript.setAuthorizations(graph.createAuthorizations(authorizations.split(",")));
        }
        VertexiumScript.setTime(time);
        compilerConfiguration.setScriptBaseClass(VertexiumScript.class.getName());

        Binding binding = new Binding();

        GroovyShell groovyShell = new GroovyShell(this.getClass().getClassLoader(), binding, compilerConfiguration);
        Closure<Object> resultHook = new Closure<Object>(this) {
            @Override
            public Object call(Object... args) {
                Object obj = args[0];
                boolean showLastResult = !io.isQuiet() && (io.isVerbose() || Preferences.getShowLastResult());
                if (showLastResult) {
                    // avoid String.valueOf here because it bypasses pretty-printing of Collections,
                    // e.g. String.valueOf( ['a': 42] ) != ['a': 42].toString()
                    io.out.println("@|bold ===>|@ " + VertexiumScript.resultToString(obj));
                }
                return null;
            }
        };

        groovysh = new Groovysh(io);
        setGroovyShell(groovysh, groovyShell);
        setResultHook(groovysh, resultHook);

        startGroovysh(evalString, fileNames);
        return 0;
    }

    private void setGroovyShell(Groovysh groovysh, GroovyShell groovyShell) throws NoSuchFieldException, IllegalAccessException {
        Field interpField = groovysh.getClass().getDeclaredField("interp");
        interpField.setAccessible(true);

        Field shellField = Interpreter.class.getDeclaredField("shell");
        shellField.setAccessible(true);

        Interpreter interpreter = (Interpreter) interpField.get(groovysh);
        shellField.set(interpreter, groovyShell);
    }

    private void setResultHook(Groovysh groovysh, Closure<Object> resultHook) throws NoSuchFieldException, IllegalAccessException {
        Field resultHookField = Groovysh.class.getDeclaredField("resultHook");
        resultHookField.setAccessible(true);

        resultHookField.set(groovysh, resultHook);
    }

    public Groovysh getGroovysh() {
        return groovysh;
    }

    public static void main(final String[] args) throws Exception {
        int result = new VertexiumShell().run(args);
        System.exit(result);
    }

    /**
     * @param evalString commands that will be executed at startup after loading files given with filenames param
     * @param filenames  files that will be loaded at startup
     */
    protected void startGroovysh(String evalString, List<String> filenames) throws IOException {
        int code;
        final Groovysh shell = getGroovysh();

        // Add a hook to display some status when shutting down...
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                //
                // FIXME: We need to configure JLine to catch CTRL-C for us... Use gshell-io's InputPipe
                //

                if (shell.getHistory() != null) {
                    try {
                        shell.getHistory().flush();
                    } catch (IOException e) {
                        System.out.println("Could not flush history.");
                    }
                }
            }
        });

        SecurityManager psm = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());

        shell.register(new GetAuthsCommand(shell));
        shell.register(new SetAuthsCommand(shell));
        shell.register(new GetTimeCommand(shell));
        shell.register(new SetTimeCommand(shell));
        shell.register(new NowCommand(shell));

        System.out.println("  _    __          __            _");
        System.out.println(" | |  / /__  _____/ /____  _  __(_)_  ______ ___");
        System.out.println(" | | / / _ \\/ ___/ __/ _ \\| |/_/ / / / / __ `__ \\");
        System.out.println(" | |/ /  __/ /  / /_/  __/>  </ / /_/ / / / / / /   v" + getClass().getPackage().getImplementationVersion());
        System.out.println(" |___/\\___/_/   \\__/\\___/_/|_/_/\\__,_/_/ /_/ /_/");
        System.out.println("");
        System.out.println("Usage:");
        System.out.println("  vertex1=v['vertex1'] - gets the vertex with id 'v1' and assigns it to variable 'v'");
        System.out.println("  vertex1.methods      - gets the methods available on the Vertexium object");
        System.out.println("  vertex1.properties   - gets the properties available on the Vertexium object");
        System.out.println("  vertex1.delete()     - deletes the vertex v1");
        System.out.println("  p1.delete()          - deletes the property currently referenced by p1");
        System.out.println("  q.has('name', 'joe').vertices().each() { println it.id } - execute a query for all vertices with property 'name' equal to 'joe'");
        System.out.println("  g.query('apple', auths).vertices()[0]                    - execute a query for 'apple' and get the first match");
        System.out.println("");
        System.out.println("Global Properties:");
        System.out.println("  g     - the Graph object");
        System.out.println("  q     - a query object");
        System.out.println("  auths - the currently set query authorizations");
        System.out.println("  time  - the currently set query time");
        System.out.println("  now   - the current time");
        System.out.println("  v     - vertex map (usage: v['v1'])");
        System.out.println("  e     - edge map (usage: e['e1'])");
        try {
            shell.execute("import " + Visibility.class.getPackage().getName() + ".*;");
            List<String> args = ImmutableList.<String>builder()
                    .add(evalString)
                    .addAll(filenames)
                    .build();
            code = shell.run(args.toArray(new String[args.size()]));
        } finally {
            System.setSecurityManager(psm);
        }

        // Force the JVM to exit at this point, since shell could have created threads or
        // popped up Swing components that will cause the JVM to linger after we have been
        // asked to shutdown

        System.exit(code);
    }

    static void setTerminalType(String type, boolean suppressColor) {
        assert type != null;

        type = type.toLowerCase();
        boolean enableAnsi = true;
        switch (type) {
            case TerminalFactory.AUTO:
                type = null;
                break;
            case TerminalFactory.UNIX:
                type = UnixTerminal.class.getCanonicalName();
                break;
            case TerminalFactory.WIN:
            case TerminalFactory.WINDOWS:
                type = WindowsTerminal.class.getCanonicalName();
                break;
            case TerminalFactory.FALSE:
            case TerminalFactory.OFF:
            case TerminalFactory.NONE:
                type = UnsupportedTerminal.class.getCanonicalName();
                // Disable ANSI, for some reason UnsupportedTerminal reports ANSI as enabled, when it shouldn't
                enableAnsi = false;
                break;
            default:
                // Should never happen
                throw new IllegalArgumentException("Invalid Terminal type: $type");
        }
        if (enableAnsi) {
            installAnsi(); // must be called before IO(), since it modifies System.in
            Ansi.setEnabled(!suppressColor);
        } else {
            Ansi.setEnabled(false);
        }

        if (type != null) {
            System.setProperty(TerminalFactory.JLINE_TERMINAL, type);
        }
    }

    static void installAnsi() {
        // Install the system adapters, replaces System.out and System.err
        // Must be called before using IO(), because IO stores refs to System.out and System.err
        AnsiConsole.systemInstall();

        // Register jline ansi detector
        Ansi.setDetector(new AnsiDetector());
    }
}
