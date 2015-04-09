package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.cli.VertexiumScript;

import java.util.List;

public class NowCommand extends CommandSupport {
    public NowCommand(Groovysh shell) {
        super(shell, ":now", ":tn");
    }

    @Override
    public Object execute(List<String> args) {
        return VertexiumScript.getTimeString(System.currentTimeMillis());
    }
}
