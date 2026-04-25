import * as DialogPrimitive from '@radix-ui/react-dialog'
import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'

import { apiPost } from '../api/client'
import { useToast } from './ToastProvider'

export interface WatchlistEditItem {
  ts_code: string
  name?: string
  type: 'holding' | 'focus'
  cost?: number | null
  shares?: number | null
  note?: string | null
}

interface WatchlistEditDialogProps {
  open: boolean
  onClose: () => void
  initialItem?: WatchlistEditItem | null
}

function WatchlistEditForm({ initialItem, onClose }: { initialItem?: WatchlistEditItem | null, onClose: () => void }) {
  const [type, setType] = useState<'holding' | 'focus'>(initialItem?.type ?? 'holding')
  const [tsCode, setTsCode] = useState(initialItem?.ts_code ?? '')
  const [name, setName] = useState(initialItem?.name ?? '')
  const [cost, setCost] = useState(initialItem?.cost !== undefined && initialItem?.cost !== null ? String(initialItem.cost) : '')
  const [shares, setShares] = useState(initialItem?.shares !== undefined && initialItem?.shares !== null ? String(initialItem.shares) : '')
  const [note, setNote] = useState(initialItem?.note ?? '')
  
  const { pushToast } = useToast()
  const queryClient = useQueryClient()
  const isEditing = Boolean(initialItem?.ts_code)

  const mutation = useMutation({
    mutationFn: async () => {
      return apiPost('/api/watchlist-config/items', {
        ts_code: tsCode,
        name: name,
        type: type,
        cost: cost ? parseFloat(cost) : undefined,
        shares: shares ? parseInt(shares, 10) : undefined,
        note: note || undefined,
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries()
      pushToast({ tone: 'success', title: isEditing ? '修改成功' : '添加成功' })
      onClose()
    },
    onError: (error: Error) => {
      pushToast({ tone: 'error', title: isEditing ? '修改失败' : '添加失败', description: error.message })
    }
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!tsCode) {
      pushToast({ tone: 'error', title: '代码不能为空' })
      return
    }
    mutation.mutate()
  }

  return (
    <>
      <div className="flex flex-col space-y-1.5 text-center sm:text-left">
        <DialogPrimitive.Title className="text-lg font-bold leading-none tracking-tight flex items-center gap-2">
          <i className={`ph-fill ${isEditing ? 'ph-pencil-simple' : 'ph-plus-circle'} text-erp-primary`}></i> 
          {isEditing ? '修改持仓/观察池' : '添加持仓/观察池'}
        </DialogPrimitive.Title>
        <DialogPrimitive.Description className="text-sm text-gray-500">
          {isEditing ? '修改标的信息后将自动同步到数据库。' : '新添加的标的会自动同步到数据库，并拉取最新行情。'}
        </DialogPrimitive.Description>
      </div>
      
      <form onSubmit={handleSubmit} className="flex flex-col gap-5 py-4">
        <div className="flex items-center gap-4">
           <label className={`flex items-center gap-2 ${isEditing ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}`}>
             <input type="radio" name="type" disabled={isEditing} checked={type === 'holding'} onChange={() => setType('holding')} className="accent-erp-primary" />
             <span className="font-bold text-gray-700">实盘持仓</span>
           </label>
           <label className={`flex items-center gap-2 ${isEditing ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}`}>
             <input type="radio" name="type" disabled={isEditing} checked={type === 'focus'} onChange={() => setType('focus')} className="accent-erp-primary" />
             <span className="font-bold text-gray-700">重点观察</span>
           </label>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div className="flex flex-col gap-2">
            <label className="text-xs font-bold uppercase text-gray-500">代码 (TS_CODE) *</label>
            <input 
              type="text" required 
              placeholder="如: 600519.SH"
              disabled={isEditing}
              value={tsCode} onChange={(e) => setTsCode(e.target.value.toUpperCase())}
              className={`h-9 px-3 rounded border erp-border bg-gray-50 outline-none transition-all uppercase font-mono ${isEditing ? 'cursor-not-allowed opacity-70' : 'focus:bg-white focus:ring-2 focus:ring-erp-primary/20'}`} 
            />
          </div>
          <div className="flex flex-col gap-2">
            <label className="text-xs font-bold uppercase text-gray-500">名称 (Name)</label>
            <input 
              type="text" 
              placeholder="如: 贵州茅台"
              value={name} onChange={(e) => setName(e.target.value)}
              className="h-9 px-3 rounded border erp-border bg-gray-50 focus:bg-white outline-none focus:ring-2 focus:ring-erp-primary/20 transition-all" 
            />
          </div>
        </div>

        {type === 'holding' ? (
          <div className="grid grid-cols-2 gap-4">
            <div className="flex flex-col gap-2">
              <label className="text-xs font-bold uppercase text-gray-500">成本价 (Cost)</label>
              <input 
                type="number" step="0.001" 
                placeholder="选填"
                value={cost} onChange={(e) => setCost(e.target.value)}
                className="h-9 px-3 rounded border erp-border bg-gray-50 focus:bg-white outline-none focus:ring-2 focus:ring-erp-primary/20 transition-all font-mono" 
              />
            </div>
            <div className="flex flex-col gap-2">
              <label className="text-xs font-bold uppercase text-gray-500">持仓股数 (Shares)</label>
              <input 
                type="number" 
                placeholder="选填"
                value={shares} onChange={(e) => setShares(e.target.value)}
                className="h-9 px-3 rounded border erp-border bg-gray-50 focus:bg-white outline-none focus:ring-2 focus:ring-erp-primary/20 transition-all font-mono" 
              />
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-2">
            <label className="text-xs font-bold uppercase text-gray-500">观察备忘 (Note)</label>
            <textarea 
              rows={3}
              placeholder="写下你关注该标的的核心理由或买入点位..."
              value={note} onChange={(e) => setNote(e.target.value)}
              className="p-3 rounded border erp-border bg-gray-50 focus:bg-white outline-none focus:ring-2 focus:ring-erp-primary/20 transition-all text-sm resize-none" 
            />
          </div>
        )}
        
        <div className="flex justify-end gap-3 mt-4 pt-4 border-t erp-border">
          <button type="button" onClick={onClose} className="px-4 py-2 rounded text-gray-500 hover:bg-gray-100 transition-colors font-bold text-sm">
            取消
          </button>
          <button type="submit" disabled={mutation.isPending} className="px-6 py-2 rounded bg-erp-primary hover:bg-erp-primary-hover text-white font-bold text-sm shadow-sm transition-all active:scale-95 disabled:opacity-50 flex items-center gap-2">
            {mutation.isPending ? <i className="ph ph-spinner animate-spin"></i> : <i className="ph ph-check"></i>}
            确认保存
          </button>
        </div>
      </form>
    </>
  )
}

export function WatchlistEditDialog({ open, onClose, initialItem }: WatchlistEditDialogProps) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 bg-black/50 z-[100] backdrop-blur-sm" />
        <DialogPrimitive.Content className="fixed left-[50%] top-[50%] z-[100] grid w-full max-w-lg translate-x-[-50%] translate-y-[-50%] gap-4 border bg-white p-6 shadow-lg duration-200 rounded-lg text-erp">
          <WatchlistEditForm key={initialItem?.ts_code ?? 'new'} initialItem={initialItem} onClose={onClose} />
          <DialogPrimitive.Close className="absolute right-4 top-4 rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 outline-none">
            <i className="ph ph-x text-lg"></i>
            <span className="sr-only">Close</span>
          </DialogPrimitive.Close>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}
