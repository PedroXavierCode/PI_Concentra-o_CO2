<script>
 export let appConfig;
 export let allowJoin;
 async function sendConfig() {
     const res = await fetch(location.origin + "/api/config", {
         method: 'POST',
         body: JSON.stringify(appConfig)
     })
 };
</script>

<form class="row mx-auto offset-lg-2">
    <div class="row mb-3">
        <label for="httpPort" class="col-sm-2 col-form-label">Porta para o Servidor</label>
        <div class="col-sm-4">
            <input bind:value={appConfig.http.port} type="number" class="form-control" id="httpPort">
        </div>
    </div>
    <div class="row mb-3">
        <label for="mqttPort" class="col-sm-2 col-form-label">Porta para o Cliente MQTT</label>
        <div class="col-sm-4">
            <input bind:value={appConfig.mqtt.port} type="number" class="form-control" id="mqttPort">
        </div>
    </div>
    <div class="row mb-3">
        <label for="mqttHost" class="col-sm-2 col-form-label">Endereço para o Cliente MQTT</label>
        <div class="col-sm-4">
            <input bind:value={appConfig.mqtt.host} type="text" class="form-control" id="mqttHost">
        </div>
    </div>
    <div class="row mb-3">
        <label for="mqttId" class="col-sm-2 col-form-label">Identificador para o Cliente MQTT</label>
        <div class="col-sm-4">
            <input bind:value={appConfig.mqtt.id} type="text" class="form-control" id="mqttPort">
        </div>
    </div>
    {#each appConfig.mqtt.topic as t}
        <div class="row mb-3">
            <label for="mqttTopic" class="col-sm-2 col-form-label">Tópico</label>
            <div class="col-sm-4">
                <input bind:value={t} type="text" class="form-control" id="mqttTopic">
            </div>
        </div>
    {/each}
    <div class="row mb-3">
        <label class="col-sm-2 col-form-label">Dispositivos</label>
        <div class="col-sm-1">
            <input bind:checked={allowJoin} type="checkbox" class="form-check-input" id="checkJoin">
        </div>
        <label class="col-sm-4 form-check-label" for="checkJoin">Permitir auto-cadastro de dispositivos</label>
    </div>
    <label class="col-sm-6 col-form-label">Template Trifásico (TF)</label>
    <textarea bind:value={appConfig.template.tf}></textarea>
    <label class="col-sm-6 col-form-label">Template Monofásico (MF)</label>
    <textarea bind:value={appConfig.template.mf}></textarea>
    <label class="col-sm-6 col-form-label">Template Monóxido de Carbono (CM)</label>
    <textarea bind:value={appConfig.template.cm}></textarea>
    <div class="row mb-3">
        <div class="col-sm-6">
            <button type="submit" class="btn btn-primary" on:click="{sendConfig}">Enviar</button>
        </div>
    </div>
</form>
