<script setup>
import { computed } from 'vue'
import Icon from './Icon.vue'

const props = defineProps({
  status: {
    type: [String, Boolean],
    default: ''
  },
  type: {
    type: String,
    required: true // 'agent', 'mode', 'approval', 'market', 'paused'
  },
  label: {
    type: String,
    default: ''
  }
})

const pillClass = computed(() => {
  const stat = String(props.status).toLowerCase().trim()
  
  if (props.type === 'agent') {
    return stat === 'true' || stat === 'online' ? 'green' : 'red'
  }
  if (props.type === 'mode') {
    return stat === 'live' ? 'orange' : 'blue'
  }
  if (props.type === 'approval') {
    return stat === 'true' || stat === 'required' || stat === 'on' ? 'orange' : 'green'
  }
  if (props.type === 'paused') {
    return stat === 'true' || stat === 'paused' ? 'yellow' : 'green'
  }
  if (props.type === 'market') {
    if (stat === 'regular' || stat === 'open') return 'green'
    if (stat === 'pre_market' || stat === 'after_hours') return 'yellow'
    return 'red'
  }
  return 'blue'
})

// Icon shown before the label, for states that previously used an emoji.
// Agent/market keep their geometric ●/◑ dot in the text (no icon).
const pillIcon = computed(() => {
  const stat = String(props.status).toLowerCase().trim()
  if (props.label) return ''
  if (props.type === 'approval') {
    return stat === 'true' || stat === 'required' || stat === 'on' ? 'lock' : 'bolt'
  }
  if (props.type === 'paused') {
    return stat === 'true' || stat === 'paused' ? 'pause' : 'play'
  }
  return ''
})

const pillText = computed(() => {
  if (props.label) return props.label
  const stat = String(props.status).toUpperCase().trim()

  if (props.type === 'agent') {
    return stat === 'TRUE' || stat === 'ONLINE' ? '● ONLINE' : '● OFFLINE'
  }
  if (props.type === 'approval') {
    return stat === 'TRUE' || stat === 'REQUIRED' || stat === 'ON' ? 'APPROVAL ON' : 'AUTO'
  }
  if (props.type === 'paused') {
    return stat === 'TRUE' || stat === 'PAUSED' ? 'PAUSED' : 'RUNNING'
  }
  if (props.type === 'market') {
    if (stat === 'REGULAR' || stat === 'OPEN') return '● OPEN'
    if (stat === 'PRE_MARKET') return '◑ PRE-MKT'
    if (stat === 'AFTER_HOURS') return '◑ AFTER-HRS'
    return '● CLOSED'
  }
  return stat
})
</script>

<template>
  <span class="pill" :class="pillClass">
    <Icon v-if="pillIcon" :name="pillIcon" :size="12" />
    {{ pillText }}
  </span>
</template>

<style scoped>
.pill {
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}
</style>
