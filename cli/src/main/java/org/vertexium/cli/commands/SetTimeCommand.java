package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.cli.VertexiumScript;

import java.util.List;

public class SetTimeCommand extends CommandSupport {
    public SetTimeCommand(Groovysh shell) {
        super(shell, ":settime", ":st");
    }

    @Override
    public Object execute(List<String> args) {
        if (args.size() == 0) {
            VertexiumScript.setTime(null);
        } else {
            VertexiumScript.setTime(Long.parseLong(args.get(0)));
        }
        return VertexiumScript.getTimeString();
    }
}
