const { app } = require('@azure/functions');

app.timer('CollectPoe2Prices', {
    schedule: '0 0 * * * *',
    handler: async (myTimer, context) => {
        context.log('PoE 2 currency collector started');
        context.log(`Execution time: ${new Date().toISOString()}`);
        context.log('PoE 2 currency collector completed');
    }
});
