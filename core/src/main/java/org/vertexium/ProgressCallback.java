package org.vertexium;

public abstract class ProgressCallback {
    public void progress(double progressPercent, Step step) {
        progress(progressPercent, step, null, null);
    }

    public abstract void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount);

    public static enum Step {
        COMPLETE("Complete"),
        SEARCHING_SOURCE_VERTEX_EDGES("Searching source vertex edges"),
        SEARCHING_DESTINATION_VERTEX_EDGES("Searching destination vertex edges"),
        MERGING_EDGES("Merging edges"),
        ADDING_PATHS("Adding paths"),
        SEARCHING_EDGES("Searching edges %d of %d"),
        FINDING_PATH("Finding path");

        private final String messageFormat;

        Step(String messageFormat) {
            this.messageFormat = messageFormat;
        }

        public String formatMessage(Integer edgeIndex, Integer vertexCount) {
            return String.format(this.messageFormat, edgeIndex, vertexCount);
        }

        public String getMessageFormat() {
            return this.messageFormat;
        }
    }
}
