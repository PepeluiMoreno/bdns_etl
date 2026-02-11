<template>
  <div class="tarjeta-entidad" :class="`estado-${entidad.estado}`">
    <!-- Cabecera -->
    <div class="tarjeta-header">
      <div class="info-entidad">
        <h3 class="nombre-entidad">{{ nombreConEjercicio }}</h3>
        <p class="descripcion-entidad">{{ entidad.descripcion }}</p>
        <p v-if="!entidad.can_seed" class="advertencia-bloqueado">
          ⚠️ {{ entidad.seed_blocked_reason }}
        </p>
      </div>

      <!-- Iconos de acción -->
      <div class="acciones-header">
        <button
          v-if="entidad.interrupted_execution && !estaProcesando"
          @click="solicitarConfirmacion"
          class="btn-reiniciar"
          title="Reiniciar proceso interrumpido"
        >
          ↻
        </button>
        <button
          v-else
          @click="solicitarConfirmacion"
          :disabled="estaProcesando || !entidad.can_seed"
          class="icono-sync"
          :class="{ 'girando': estaProcesando }"
          :title="botonSyncTitle"
        >
          ⟳
        </button>
      </div>
    </div>

    <!-- Barra de progreso -->
    <div class="seccion-progreso">
      <div class="barra-progreso">
        <div
          class="relleno-progreso"
          :style="{ width: `${entidad.progreso}%` }"
          :class="colorProgreso"
        ></div>
      </div>
      <div class="info-progreso">
        <span class="porcentaje-progreso">{{ entidad.progreso }}%</span>
        <span class="registros-progreso">
          {{ formatearNumero(entidad.registros_procesados) }} / {{ formatearNumero(entidad.total_registros) }} registros
        </span>
      </div>
    </div>

    <!-- Información de sincronización -->
    <div class="seccion-info-sync">
      <div>
        <div class="label-sync">Última sincronización</div>
        <div class="fecha-sync">{{ entidad.ultima_sync ? formatearFecha(entidad.ultima_sync) : 'Nunca' }}</div>
      </div>
      <button
        @click="mostrarModalDetalle = true"
        :disabled="!entidad.ultima_sync"
        class="btn-ver-detalle"
      >
        Ver detalle
      </button>
    </div>

    <!-- Badge de estado abajo a la derecha -->
    <div class="contenedor-badge">
      <span class="badge-estado" :class="claseBadgeEstado">{{ textoEstadoSincronizacion }}</span>
    </div>

    <!-- Modal de confirmación -->
    <div v-if="mostrarModalConfirmacion" class="modal-overlay" @click="mostrarModalConfirmacion = false">
      <div class="modal-contenido modal-confirmacion" @click.stop>
        <div class="modal-header">
          <h3>Confirmar sincronización</h3>
          <button @click="mostrarModalConfirmacion = false" class="btn-cerrar">×</button>
        </div>
        <div class="modal-body">
          <p class="mensaje-confirmacion">
            ¿Desea iniciar la sincronización de <strong>{{ nombreConEjercicio }}</strong>?
          </p>
          <p class="mensaje-info">
            Puede seguir el progreso del proceso desde la entrada de menú <strong>Procesos</strong>.
          </p>
          <div class="acciones-confirmacion">
            <button @click="mostrarModalConfirmacion = false" class="btn-cancelar">
              Cancelar
            </button>
            <button @click="confirmarPoblar" class="btn-confirmar">
              Iniciar sincronización
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Modal de detalle -->
    <div v-if="mostrarModalDetalle" class="modal-overlay" @click="mostrarModalDetalle = false">
      <div class="modal-contenido" @click.stop>
        <div class="modal-header">
          <h3>Detalle de sincronización</h3>
          <button @click="mostrarModalDetalle = false" class="btn-cerrar">×</button>
        </div>
        <div class="modal-body">
          <div class="detalle-item">
            <span class="detalle-label">Última sincronización:</span>
            <span class="detalle-valor">{{ formatearFecha(entidad.ultima_sync) }}</span>
          </div>
          <div class="estadisticas-detalle">
            <div class="estadistica-detalle-item">
              <span class="estadistica-label">Nuevos</span>
              <span class="estadistica-valor">{{ formatearEstadistica(entidad.cambios_pendientes.nuevos) }}</span>
            </div>
            <div class="estadistica-detalle-item">
              <span class="estadistica-label">Actualizados</span>
              <span class="estadistica-valor">{{ formatearEstadistica(entidad.cambios_pendientes.actualizados) }}</span>
            </div>
            <div class="estadistica-detalle-item">
              <span class="estadistica-label">Borrados</span>
              <span class="estadistica-valor">{{ formatearEstadistica(entidad.cambios_pendientes.borrados) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  entidad: {
    type: Object,
    required: true
  },
  ejercicio: {
    type: Number,
    default: null
  }
})

const emit = defineEmits(['poblar', 'sincronizar', 'auto-resync', 'descartar'])

// Estado
const mostrarModalDetalle = ref(false)
const mostrarModalConfirmacion = ref(false)

// Computed
const nombreConEjercicio = computed(() => {
  if (!props.ejercicio) return props.entidad.nombre
  return `${props.entidad.nombre} ${props.ejercicio}`
})
const estaProcesando = computed(() => {
  return ['poblando', 'syncing', 'processing'].includes(props.entidad.estado)
})

const textoEstado = computed(() => {
  const estados = {
    pending: 'Pendiente',
    seeding: 'Procesando...',
    syncing: 'Sincronizando...',
    complete: 'Completado',
    outdated: 'Desactualizado',
    error: 'Error'
  }
  return estados[props.entidad.estado] || props.entidad.estado
})

const colorProgreso = computed(() => {
  if (props.entidad.progreso === 100) return 'progreso-completo'
  if (props.entidad.progreso >= 50) return 'progreso-bueno'
  if (props.entidad.progreso >= 25) return 'progreso-medio'
  return 'progreso-bajo'
})

const textoEstadoSincronizacion = computed(() => {
  if (props.entidad.interrupted_execution) return 'Interrumpido'
  if (props.entidad.estado === 'complete') return 'Sincronizado'
  if (!props.entidad.ultima_sync) return 'Sin sincronizar'
  return 'Pendiente sincronización'
})

const claseBadgeEstado = computed(() => {
  if (props.entidad.interrupted_execution) return 'badge-interrumpido'
  if (props.entidad.estado === 'complete') return 'badge-sincronizado'
  if (!props.entidad.ultima_sync) return 'badge-sin-sincronizar'
  return 'badge-pendiente'
})

const botonSyncTitle = computed(() => {
  if (!props.entidad.can_seed) {
    return props.entidad.seed_blocked_reason || 'No disponible para poblar'
  }
  if (estaProcesando.value) {
    return 'Proceso en ejecución...'
  }
  return 'Poblar entidad'
})

// Métodos
function solicitarConfirmacion() {
  mostrarModalConfirmacion.value = true
}

function confirmarPoblar() {
  mostrarModalConfirmacion.value = false
  const replacingId = props.entidad.interrupted_execution?.execution_id || null
  emit('poblar', props.entidad.id, replacingId)
}

function manejarSincronizar() {
  emit('sincronizar', props.entidad.id)
}

function manejarAutoResync() {
  emit('auto-resync', props.entidad.id)
}

function manejarDescartar() {
  emit('descartar', props.entidad.id)
}

function formatearNumero(num) {
  return new Intl.NumberFormat('es-ES').format(num || 0)
}

function formatearEstadistica(valor) {
  // Si nunca ha habido sincronización, mostrar guion
  if (!props.entidad.ultima_sync) {
    return '-'
  }
  return valor
}

function formatearFecha(fechaString) {
  if (!fechaString) return '-'
  const fecha = new Date(fechaString)
  return new Intl.DateTimeFormat('es-ES', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(fecha)
}
</script>

<style scoped>
.tarjeta-entidad {
  background-color: #f5f5f5;
  border: 1px solid #e0e0e0;
  border-radius: 12px;
  padding: 20px;
  transition: all 0.3s ease;
}

.tarjeta-entidad:hover {
  border-color: #4a9eff;
  box-shadow: 0 4px 12px rgba(74, 158, 255, 0.15);
}

.tarjeta-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
}

.info-entidad {
  flex: 1;
}

.nombre-entidad {
  font-size: 18px;
  font-weight: 600;
  color: #333333;
  margin: 0 0 4px 0;
}

.descripcion-entidad {
  font-size: 13px;
  color: #666666;
  margin: 0;
}

.advertencia-bloqueado {
  font-size: 12px;
  color: #f59e0b;
  background-color: #fef3c7;
  padding: 4px 8px;
  border-radius: 4px;
  margin: 8px 0 0 0;
  border-left: 3px solid #f59e0b;
  font-weight: 500;
}

.icono-sync {
  background: transparent;
  border: none;
  font-size: 32px;
  color: #4a9eff;
  cursor: pointer;
  padding: 4px;
  transition: all 0.2s ease;
}

.icono-sync:hover:not(:disabled) {
  color: #357abd;
  transform: rotate(180deg);
}

.icono-sync:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.icono-sync.girando {
  animation: girar-continuo 1.5s linear infinite;
}

.acciones-header {
  display: flex;
  align-items: center;
  gap: 4px;
}

.btn-reiniciar {
  background: transparent;
  border: 2px solid #f97316;
  font-size: 28px;
  color: #f97316;
  cursor: pointer;
  padding: 2px 6px;
  border-radius: 8px;
  transition: all 0.2s ease;
  line-height: 1;
}

.btn-reiniciar:hover {
  background-color: #f97316;
  color: white;
}

@keyframes girar-continuo {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

/* Barra de progreso */
.seccion-progreso {
  margin-bottom: 16px;
}

.barra-progreso {
  height: 8px;
  background-color: #e0e0e0;
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 8px;
}

.relleno-progreso {
  height: 100%;
  transition: width 0.5s ease, background-color 0.3s ease;
  border-radius: 4px;
}

.progreso-bajo { background: linear-gradient(90deg, #e74c3c, #c0392b); }
.progreso-medio { background: linear-gradient(90deg, #f39c12, #d68910); }
.progreso-bueno { background: linear-gradient(90deg, #3498db, #2980b9); }
.progreso-completo { background: linear-gradient(90deg, #2ecc71, #27ae60); }

.info-progreso {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}

.porcentaje-progreso {
  font-weight: 600;
  color: #333333;
}

.registros-progreso {
  color: #666666;
}

/* Sección de información de sincronización */
.seccion-info-sync {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  background-color: #ffffff;
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  margin-bottom: 12px;
}

.label-sync {
  font-size: 11px;
  color: #666666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 4px;
}

.fecha-sync {
  font-size: 14px;
  color: #333333;
  font-weight: 500;
}

.btn-ver-detalle {
  padding: 8px 16px;
  background-color: #4a9eff;
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-ver-detalle:hover:not(:disabled) {
  background-color: #357abd;
}

.btn-ver-detalle:disabled {
  background-color: #cccccc;
  cursor: not-allowed;
  opacity: 0.6;
}

/* Contenedor del badge (abajo a la derecha) */
.contenedor-badge {
  display: flex;
  justify-content: flex-end;
}

/* Badge de estado */
.badge-estado {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.3px;
  white-space: nowrap;
}

.badge-sincronizado {
  background-color: #10b981;
  color: #ffffff;
}

.badge-pendiente {
  background-color: #f59e0b;
  color: #ffffff;
}

.badge-sin-sincronizar {
  background-color: #ef4444;
  color: #ffffff;
}

.badge-interrumpido {
  background-color: #f97316;
  color: #ffffff;
}

/* Modal */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-contenido {
  background: white;
  border-radius: 12px;
  padding: 0;
  max-width: 500px;
  width: 90%;
  max-height: 80vh;
  overflow-y: auto;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px;
  border-bottom: 1px solid #e0e0e0;
}

.modal-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #333333;
}

.btn-cerrar {
  background: none;
  border: none;
  font-size: 28px;
  color: #666666;
  cursor: pointer;
  line-height: 1;
  padding: 0;
  width: 32px;
  height: 32px;
}

.btn-cerrar:hover {
  color: #333333;
}

.modal-body {
  padding: 20px;
}

.detalle-item {
  margin-bottom: 20px;
  padding-bottom: 20px;
  border-bottom: 1px solid #e0e0e0;
}

.detalle-label {
  font-size: 12px;
  color: #666666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  display: block;
  margin-bottom: 4px;
}

.detalle-valor {
  font-size: 14px;
  color: #333333;
  font-weight: 500;
}

.estadisticas-detalle {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.estadistica-detalle-item {
  text-align: center;
  padding: 12px;
  background-color: #f9fafb;
  border-radius: 8px;
}

.estadistica-label {
  font-size: 11px;
  color: #666666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  display: block;
  margin-bottom: 8px;
}

.estadistica-valor {
  font-size: 24px;
  font-weight: 700;
  color: #333333;
}

/* Modal de confirmación */
.modal-confirmacion {
  max-width: 450px;
}

.mensaje-confirmacion {
  font-size: 15px;
  color: #333333;
  margin-bottom: 16px;
  line-height: 1.5;
}

.mensaje-info {
  font-size: 13px;
  color: #666666;
  background-color: #f0f9ff;
  padding: 12px;
  border-radius: 6px;
  border-left: 3px solid #4a9eff;
  margin-bottom: 20px;
  line-height: 1.5;
}

.acciones-confirmacion {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
}

.btn-cancelar,
.btn-confirmar {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-cancelar {
  background-color: #e0e0e0;
  color: #333333;
}

.btn-cancelar:hover {
  background-color: #d0d0d0;
}

.btn-confirmar {
  background-color: #4a9eff;
  color: white;
}

.btn-confirmar:hover {
  background-color: #357abd;
}

</style>
