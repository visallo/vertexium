package org.vertexium.cli.commands;

import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.vertexium.Authorizations;
import org.vertexium.cli.VertexiumScript;

import java.util.List;

public class SetAuthsCommand extends CommandSupport {
    public SetAuthsCommand(Groovysh shell) {
        super(shell, ":setauths", ":sa");
    }

    @Override
    public Object execute(List<String> args) {
        Authorizations authorizations = VertexiumScript.getGraph().createAuthorizations(args.toArray(new String[args.size()]));
        VertexiumScript.setAuthorizations(authorizations);
        return authorizations;
    }
}
