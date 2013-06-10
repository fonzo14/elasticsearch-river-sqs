package org.elasticsearch.river.sqs;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
*
*/
public class SqsRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(SqsRiver.class).asEagerSingleton();
    }
}