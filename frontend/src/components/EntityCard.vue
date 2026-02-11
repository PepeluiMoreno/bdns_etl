<template>
  <div class="entity-card" :class="`estado-${entity.estado}`">
    <!-- Header -->
    <div class="card-header">
      <div class="entity-info">
        <span class="entity-icon">{{ entity.icono }}</span>
        <div>
          <h3 class="entity-nombre">{{ entity.nombre }}</h3>
          <p class="entity-descripcion">{{ entity.descripcion }}</p>
        </div>
      </div>

      <!-- Badge de estado -->
      <span class="estado-badge" :class="`badge-${entity.estado}`">
        {{ estadoTexto }}
      </span>
    </div>

    <!-- Barra de progreso -->
    <div class="progress-section">
      <div class="progress-bar">
        <div
          class="progress-fill"
          :style="{ width: `${entity.progreso}%` }"
          :class="progresoColor"
        ></div>
      </div>
      <div class="progress-info">
        <span class="progress-percentage">{{ entity.progreso }}%</span>
        <span class="progress-records">
          {{ formatNumber(entity.registros_procesados) }} / {{ formatNumber(entity.total_registros) }} registros
        </span>
      </div>
    </div>

    <!-- Cambios pendientes (si los hay) -->
    <div v-if="tieneCambios" class="cambios-section">
      <div class="cambios-header">
        <span class="cambios-icon">⚠️</span>
        <span class="cambios-title">Cambios detectados</span>
      </div>
      <div class="cambios-stats">
        <span v-if="entity.cambios_pendientes.nuevos > 0" class="cambio-badge nuevos">
          +{{ entity.cambios_pendientes.nuevos }} nuevos
        </span>
        <span v-if="entity.cambios_pendientes.actualizados > 0" class="cambio-badge actualizados">
          ~{{ entity.cambios_pendientes.actualizados }} actualizados
        </span>
        <span v-if="entity.cambios_pendientes.borrados > 0" class="cambio-badge borrados">
          -{{ entity.cambios_pendientes.borrados }} borrados
        </span>
      </div>

      <!-- Auto-resync eligible -->
      <div v-if="entity.auto_resync_eligible" class="auto-resync-banner">
        <span class="auto-icon">🤖</span>
        <span>Elegible para resincronización automática</span>
      </div>
    </div>

    <!-- Última sincronización -->
    <div v-if="entity.ultima_sync" class="ultima-sync">
      <span class="sync-label">Última sync:</span>
      <span class="sync-date">{{ formatDate(entity.ultima_sync) }}</span>
    </div>

    <!-- Acciones -->
    <div class="card-actions">
      <!-- Botón Seed -->
      <button
        v-if="entity.progreso < 100 || entity.estado === 'outdated'"
        @click="handleSeed"
        :disabled="isProcessing"
        class="action-btn btn-seed"
      >
        <span class="btn-icon">▶️</span>
        <span>{{ entity.progreso === 0 ? 'Iniciar Seeding' : 'Reseed' }}</span>
      </button>

      <!-- Botón Sync -->
      <button
        @click="handleSync"
        :disabled="isProcessing"
        class="action-btn btn-sync"
      >
        <span class="btn-icon">🔄</span>
        <span>Sincronizar</span>
      </button>

      <!-- Botón Auto-resync -->
      <button
        v-if="entity.auto_resync_eligible"
        @click="handleAutoResync"
        :disabled="isProcessing"
        class="action-btn btn-auto-resync"
      >
        <span class="btn-icon">🤖</span>
        <span>Auto-resync</span>
      </button>

      <!-- Botón Descartar cambios -->
      <button
        v-if="tieneCambios"
        @click="handleDismiss"
        :disabled="isProcessing"
        class="action-btn btn-dismiss"
      >
        <span class="btn-icon">✖️</span>
        <span>Ignorar</span>
      </button>

      <!-- Indicador de completado -->
      <div v-if="entity.progreso === 100 && !tieneCambios" class="completed-indicator">
        <span class="completed-icon">✅</span>
        <span>Completado</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  entity: {
    type: Object,
    required: true
  }
})

const emit = defineEmits(['seed', 'sync', 'auto-resync', 'dismiss'])

// Computed
const isProcessing = computed(() => {
  return ['seeding', 'syncing'].includes(props.entity.estado)
})

const tieneCambios = computed(() => {
  const { nuevos, actualizados, borrados } = props.entity.cambios_pendientes
  return nuevos > 0 || actualizados > 0 || borrados > 0
})

const estadoTexto = computed(() => {
  const estados = {
    pending: 'Pendiente',
    seeding: 'Procesando...',
    syncing: 'Sincronizando...',
    complete: 'Completado',
    outdated: 'Desactualizado',
    error: 'Error'
  }
  return estados[props.entity.estado] || props.entity.estado
})

const progresoColor = computed(() => {
  if (props.entity.progreso === 100) return 'progress-complete'
  if (props.entity.progreso >= 50) return 'progress-good'
  if (props.entity.progreso >= 25) return 'progress-medium'
  return 'progress-low'
})

// Methods
function handleSeed() {
  emit('seed', props.entity.id)
}

function handleSync() {
  emit('sync', props.entity.id)
}

function handleAutoResync() {
  emit('auto-resync', props.entity.id)
}

function handleDismiss() {
  emit('dismiss', props.entity.id)
}

function formatNumber(num) {
  return new Intl.NumberFormat('es-ES').format(num || 0)
}

function formatDate(dateString) {
  if (!dateString) return '-'
  const date = new Date(dateString)
  return new Intl.DateTimeFormat('es-ES', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date)
}
</script>

<style scoped>
.entity-card {
  background-color: var(--bg-secondary, #1a1a1a);
  border: 1px solid #3a3a3a;
  border-radius: 12px;
  padding: 20px;
  transition: all 0.3s ease;
}

.entity-card:hover {
  border-color: #4a9eff;
  box-shadow: 0 4px 12px rgba(74, 158, 255, 0.15);
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
}

.entity-info {
  display: flex;
  gap: 12px;
  align-items: center;
}

.entity-icon {
  font-size: 32px;
}

.entity-nombre {
  font-size: 18px;
  font-weight: 600;
  color: var(--text-primary, #ffffff);
  margin: 0 0 4px 0;
}

.entity-descripcion {
  font-size: 13px;
  color: var(--text-secondary, #999999);
  margin: 0;
}

.estado-badge {
  padding: 4px 12px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: 500;
}

.badge-pending { background: #444; color: #aaa; }
.badge-seeding, .badge-syncing { background: #4a9eff; color: white; animation: pulse 2s infinite; }
.badge-complete { background: #2ecc71; color: white; }
.badge-outdated { background: #f39c12; color: white; }
.badge-error { background: #e74c3c; color: white; }

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.7; }
}

/* Progress bar */
.progress-section {
  margin-bottom: 16px;
}

.progress-bar {
  height: 8px;
  background-color: #2a2a2a;
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 8px;
}

.progress-fill {
  height: 100%;
  transition: width 0.5s ease, background-color 0.3s ease;
  border-radius: 4px;
}

.progress-low { background: linear-gradient(90deg, #e74c3c, #c0392b); }
.progress-medium { background: linear-gradient(90deg, #f39c12, #d68910); }
.progress-good { background: linear-gradient(90deg, #3498db, #2980b9); }
.progress-complete { background: linear-gradient(90deg, #2ecc71, #27ae60); }

.progress-info {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}

.progress-percentage {
  font-weight: 600;
  color: var(--text-primary, #ffffff);
}

.progress-records {
  color: var(--text-secondary, #999999);
}

/* Cambios pendientes */
.cambios-section {
  background-color: rgba(243, 156, 18, 0.1);
  border: 1px solid rgba(243, 156, 18, 0.3);
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 16px;
}

.cambios-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.cambios-icon {
  font-size: 16px;
}

.cambios-title {
  font-weight: 600;
  color: #f39c12;
  font-size: 13px;
}

.cambios-stats {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.cambio-badge {
  padding: 4px 8px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 500;
}

.cambio-badge.nuevos { background: #2ecc71; color: white; }
.cambio-badge.actualizados { background: #3498db; color: white; }
.cambio-badge.borrados { background: #e74c3c; color: white; }

.auto-resync-banner {
  margin-top: 8px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: #4a9eff;
}

.auto-icon {
  font-size: 14px;
}

/* Última sync */
.ultima-sync {
  font-size: 12px;
  color: var(--text-secondary, #999999);
  margin-bottom: 16px;
}

.sync-label {
  margin-right: 4px;
}

.sync-date {
  color: var(--text-primary, #ffffff);
  font-weight: 500;
}

/* Acciones */
.card-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.action-btn {
  flex: 1;
  min-width: 120px;
  padding: 10px 16px;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  transition: all 0.2s ease;
}

.btn-seed {
  background: linear-gradient(135deg, #4a9eff, #357abd);
  color: white;
}

.btn-seed:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(74, 158, 255, 0.4);
}

.btn-sync {
  background: linear-gradient(135deg, #9b59b6, #8e44ad);
  color: white;
}

.btn-sync:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(155, 89, 182, 0.4);
}

.btn-auto-resync {
  background: linear-gradient(135deg, #2ecc71, #27ae60);
  color: white;
}

.btn-auto-resync:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(46, 204, 113, 0.4);
}

.btn-dismiss {
  background: #444;
  color: #ccc;
  flex: 0 0 auto;
  min-width: auto;
}

.btn-dismiss:hover:not(:disabled) {
  background: #555;
}

.action-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-icon {
  font-size: 14px;
}

.completed-indicator {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 10px;
  background-color: rgba(46, 204, 113, 0.1);
  border: 1px solid rgba(46, 204, 113, 0.3);
  border-radius: 8px;
  color: #2ecc71;
  font-weight: 600;
  font-size: 13px;
}

.completed-icon {
  font-size: 18px;
}
</style>
