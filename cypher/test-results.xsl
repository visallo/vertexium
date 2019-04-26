<stylesheet xmlns="http://www.w3.org/1999/XSL/Transform" version="1.0">
    <output method="text"/>
    <variable name="vSpace" select="'                                       '"/>

    <template match="/">
        <apply-templates select="//suite"/>
    </template>

    <template match="//suite">
        <variable name="depth" select="count(ancestor::suite)"/>
        <value-of select="substring($vSpace, 1, $depth)"/>
        <value-of select="@name"/>
        <text>&#xa;</text>
        <apply-templates select="test"/>
    </template>

    <template match="test">
        <text>   </text>
        <value-of select="@status"/> -
        <value-of select="@name"/>
        <text>&#xa;</text>
    </template>
</stylesheet>