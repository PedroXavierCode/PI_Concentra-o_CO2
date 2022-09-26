<script>
import { onMount } from 'svelte';
import Device from '../Components/Device/Device.svelte'
import Config from '../Components/Config/Config.svelte'
$: appConfig = {"http": {"port": 0}, "mqtt": {"host": "", "port": 0, "topic": [], "id": ""}, "template": {"tf": "", "mf": "", "cm": ""}};
$: allowJoin = true;
$: deviceList = {};
$: cfg = true;
$: login = "";
$: passwd = "";
$: logged = false;
onMount(async () => {
    fetch(location.origin + "/api/config")
        .then(response => response.json())
        .then(data => {
            appConfig = data
        }).catch(error => {
            return [];
        });
    fetch(location.origin + "/api/device")
        .then(response => response.json())
        .then(data => {
            deviceList = data;
        }).catch(error => {
            return [];
        });
});
function onLogin() {
    if (login == "admin" && passwd == "admin123") {
        logged = true;
    }
};
</script>

<svelte:head>
    <title>IoT Middleware</title>
</svelte:head>

<html lang="en">
    <body class="bg-light">
        <div class="container-fluid"><div class="d-flex justify-content-center">
            <div class="row flex-nowrap">
                {#if logged}
                    <div class="col-auto px-0">
                        <div id="sidebar" class="collapse collapse-horizontal show border-end">
                            <div id="sidebar-nav" class="list-group border-0 rounded-0 text-sm-start min-vh-100">
                                <a href="#" on:click="{() => (cfg = true)}" class="list-group-item border-end-0 d-inline-block text-truncate" data-bs-parent="#sidebar"><span>Configurações</span> </a>
                                <a href="#" on:click="{() => (cfg = false)}" class="list-group-item border-end-0 d-inline-block text-truncate" data-bs-parent="#sidebar"><span>Dispositivos</span> </a>
                            </div>
                        </div>
                    </div>
                    <main class="col ps-md-2 pt-2">
                        <a href="#" data-bs-target="#sidebar" data-bs-toggle="collapse" class="border rounded-3 p-1 text-decoration-none"><i class="bi bi-list bi-lg py-2 p-1"></i> Menu</a>
                        <div class="row">
                            <div class="col-12">
                                <h1 class="bg-secondary text-light text-center">IoT Middleware</h1>
                                <div class="container">
                                    {#if cfg}
                                        <Config {appConfig} {allowJoin}></Config>
                                    {:else}
                                        <Device {deviceList}></Device>
                                    {/if}
                                </div>
                            </div>
                        </div>
                    </main>
                {:else}
                    <div class="container">
                        <img src="./iot.png" class="img-fluid" alt="iot">
                        <form class="row mx-auto offset-lg-2">
                            <div class="row mb-3">
                                <label for="log" class="col-sm-2 col-form-label">Login</label>
                                <div class="col-sm-4">
                                    <input bind:value={login} type="text" class="form-control" id="log">
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="pass" class="col-sm-2 col-form-label">Senha</label>
                                <div class="col-sm-4">
                                    <input bind:value={passwd} type="password" class="form-control" id="pass">
                                </div>
                            </div>
                            <div class="row mb-3">
                                <div class="col-sm-6">
                                    <button type="submit" class="btn btn-primary" on:click="{onLogin}">Enviar</button>
                                </div>
                            </div>
                        </form>
                    </div>
                {/if}
            </div>
        </div></div>
    </body>
</html>
