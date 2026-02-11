<template>
  <div class="dashboard-poblamiento">
    <!-- Selector de Ejercicio -->
    <SelectorEjercicio
      v-model="ejercicioActual"
      :ejercicios="ejerciciosDisponibles"
      @update:modelValue="manejarActualizar"
    />

    <!-- Estado de carga -->
    <div v-if="estaCargando && entidades.length === 0" class="estado-carga">
      <div class="spinner"></div>
      <p>Cargando estado de entidades...</p>
    </div>

    <!-- Estado de error -->
    <div v-else-if="error" class="estado-error">
      <span class="error-icon">❌</span>
      <p>{{ error }}</p>
      <button @click="manejarActualizar" class="btn-reintentar">Reintentar</button>
    </div>

    <!-- Rejilla de entidades -->
    <div v-else class="rejilla-entidades">
      <!-- Catálogos: tarjeta fija, sin ejercicio -->
      <TarjetaEntidad
        v-if="entidadCatalogos"
        :entidad="entidadCatalogos"
        @poblar="manejarPoblar"
      />

      <!-- Entidades vinculadas al ejercicio -->
      <TarjetaEntidad
        v-for="entidad in entidadesDelEjercicio"
        :key="entidad.id"
        :entidad="entidad"
        :ejercicio="ejercicioActual"
        @poblar="manejarPoblar"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useETLStore } from '@/stores/etl'
import SelectorEjercicio from '@/components/SelectorEjercicio.vue'
import TarjetaEntidad from '@/components/TarjetaEntidad.vue'

const etlStore = useETLStore()

let intervaloActualizacion = null

// Estado del selector de ejercicio
const ejercicioActual = ref(new Date().getFullYear())
const ejerciciosDisponibles = ref([2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026])

// Computed
const entidades = computed(() => etlStore.entities)
const entidadCatalogos = computed(() => entidades.value.find(e => e.id === 'catalogos'))
const entidadesDelEjercicio = computed(() => entidades.value.filter(e => !e.atemporal))
const estaCargando = computed(() => etlStore.loading)
const error = computed(() => etlStore.error)

const estaProcesando = computed(() => {
  return entidades.value.some(e => ['seeding', 'syncing'].includes(e.estado))
})

// Métodos
async function manejarActualizar() {
  try {
    await etlStore.loadEntitiesStatus(ejercicioActual.value)
  } catch (err) {
    console.error('Error al actualizar:', err)
  }
}

async function manejarPoblar(entidadId, replacingExecutionId = null) {
  try {
    await etlStore.startEntitySeeding(entidadId, ejercicioActual.value, replacingExecutionId)
    console.log(`Sincronización iniciada para ${entidadId} en ejercicio ${ejercicioActual.value}`)
  } catch (err) {
    console.error('Error al iniciar sincronización:', err)
    alert(`Error al iniciar sincronización: ${err.response?.data?.detail || err.message}`)
  }
}

// Ciclo de vida
onMounted(async () => {
  // Cargar estado inicial
  await manejarActualizar()

  // Actualización automática cada 30 segundos
  intervaloActualizacion = setInterval(() => {
    if (!estaProcesando.value) {
      manejarActualizar()
    }
  }, 30000)
})

onUnmounted(() => {
  if (intervaloActualizacion) {
    clearInterval(intervaloActualizacion)
  }
})
</script>

<style scoped>
.dashboard-poblamiento {
  padding: 24px;
  max-width: 1600px;
  margin: 0 auto;
}

/* Estados de carga/error */
.estado-carga,
.estado-error {
  text-align: center;
  padding: 60px 20px;
}

.spinner {
  width: 40px;
  height: 40px;
  border: 4px solid #3a3a3a;
  border-top-color: #4a9eff;
  border-radius: 50%;
  animation: spin 1s linear infinite;
  margin: 0 auto 16px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.estado-carga p,
.estado-error p {
  color: var(--text-secondary, #999999);
  font-size: 14px;
}

.error-icon {
  font-size: 48px;
  display: block;
  margin-bottom: 16px;
}

.btn-reintentar {
  margin-top: 16px;
  padding: 10px 24px;
  background: #4a9eff;
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
}

.btn-reintentar:hover {
  background: #357abd;
}

/* Rejilla de entidades */
.rejilla-entidades {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
  gap: 20px;
}

@media (max-width: 1200px) {
  .rejilla-entidades {
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
  }
}

@media (max-width: 768px) {
  .dashboard-header {
    flex-direction: column;
    gap: 16px;
  }

  .header-actions {
    width: 100%;
  }

  .btn-auto-resync-all,
  .btn-refresh {
    flex: 1;
  }

  .rejilla-entidades {
    grid-template-columns: 1fr;
  }
}
</style>
