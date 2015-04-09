package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.cli.VertexiumScript;

import java.util.List;

public class GetAuthsCommand extends CommandSupport {
    public GetAuthsCommand(Groovysh shell) {
        super(shell, ":getauths", ":ga");
    }

    @Override
    public Object execute(List<String> args) {
        return VertexiumScript.getAuthorizations();
    }
}
