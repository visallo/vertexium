package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.Authorizations;
import org.vertexium.cli.VertexiumScript;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SetAuthsCommand extends CommandSupport {
    public SetAuthsCommand(Groovysh shell) {
        super(shell, ":setauths", ":sa");
    }

    @Override
    public Object execute(List<String> args) {
        List<String> auths = parseAuths(args);
        Authorizations authorizations = VertexiumScript.getGraph().createAuthorizations(auths.toArray(new String[auths.size()]));
        VertexiumScript.setAuthorizations(authorizations);
        return authorizations;
    }

    static List<String> parseAuths(List<String> args) {
        List<String> auths = new ArrayList<>();
        for (String arg : args) {
            auths.addAll(parseAuths(arg));
        }
        return auths;
    }

    private static Collection<String> parseAuths(String arg) {
        List<String> auths = new ArrayList<>();
        for (int i = 0; i < arg.length(); i++) {
            char ch = arg.charAt(i);
            if (ch == '\'') {
                int start = i + 1;
                int end = arg.indexOf('\'', start);
                if (end == -1) {
                    throw new RuntimeException("Missing end '");
                }
                auths.add(arg.substring(start, end));
                i = end + 1;
            } else if (ch == '"') {
                int start = i + 1;
                int end = arg.indexOf('"', start);
                if (end == -1) {
                    throw new RuntimeException("Missing end \"");
                }
                auths.add(arg.substring(start, end));
                i = end + 1;
            } else if (!Character.isWhitespace(ch)) {
                int start = i;
                int end = arg.indexOf(',', start);
                if (end == -1) {
                    end = arg.length();
                }
                auths.add(arg.substring(start, end));
                i = end + 1;
            }
        }
        return auths;
    }
}
