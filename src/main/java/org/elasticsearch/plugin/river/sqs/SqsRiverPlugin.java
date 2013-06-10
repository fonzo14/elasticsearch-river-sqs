package org.elasticsearch.plugin.river.sqs;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.sqs.SqsRiverModule;

/**
*
*/
public class SqsRiverPlugin extends AbstractPlugin {

    @Inject
    public SqsRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-sqs";
    }

    @Override
    public String description() {
        return "River SQS Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("sqs", SqsRiverModule.class);
    }
}