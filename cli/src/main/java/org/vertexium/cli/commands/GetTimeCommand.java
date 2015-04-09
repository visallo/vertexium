package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.cli.VertexiumScript;

import java.util.List;

public class GetTimeCommand extends CommandSupport {
    public GetTimeCommand(Groovysh shell) {
        super(shell, ":gettime", ":gt");
    }

    @Override
    public Object execute(List<String> args) {
        return VertexiumScript.getTimeString();
    }
}
