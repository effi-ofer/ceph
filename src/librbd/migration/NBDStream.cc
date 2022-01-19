// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/NBDStream.h"
#include "common/armor.h"
#include "common/ceph_crypto.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/HttpClient.h"
#include "librbd/migration/HttpProcessorInterface.h"
#include <boost/beast/http.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/chrono.h>
#include <fmt/format.h>

#include <time.h>

namespace librbd {
namespace migration {

namespace {

const std::string URL_KEY  {"url"};
const std::string PORT_KEY {"port"};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::NBDStream: " << this \
                           << " " << __func__ << ": "

template <typename I>
NBDStream<I>::NBDStream(I* image_ctx, const json_spirit::mObject& json_object)
  : m_image_ctx(image_ctx), m_cct(image_ctx->cct),
    m_asio_engine(image_ctx->asio_engine), m_json_object(json_object) {
}

template <typename I>
NBDStream<I>::~NBDStream() {
}

template <typename I>
void NBDStream<I>::open(Context* on_finish) {
  auto& url_value = m_json_object[URL_KEY];
  if (url_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << URL_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  auto& port_value = m_json_object[PORT_KEY];
  if (port_value.type() != json_spirit::str_type) {
    lderr(m_cct) << "failed to locate '" << PORT_KEY << "' key" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  const char *m_url = &(url_value.get_str())[0];
  const char *m_port = &(port_value.get_str())[0];

  nbd = nbd_create();
  if (nbd == NULL) {
    lderr(m_cct) << "failed to create nbd object '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  if (nbd_add_meta_context (nbd, LIBNBD_CONTEXT_BASE_ALLOCATION) == -1) {
    lderr(m_cct) << "failed to add nbd meta context '" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
  if (nbd_connect_tcp(nbd, m_url, m_port) == -1) {
    lderr(m_cct) << "failed to connect to nbd server: " << nbd_get_error() 
                 << " (errno=" << nbd_get_errno() << ")" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_cct, 10) << "url=" << m_url << ", "
                   << "port=" << m_port << dendl;

  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::close(Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  if (nbd != NULL) {
    nbd_close(nbd);
  }
  on_finish->complete(0);
}

template <typename I>
void NBDStream<I>::get_size(uint64_t* size, Context* on_finish) {
  ldout(m_cct, 10) << dendl;

  *size = nbd_get_size(nbd);
  on_finish->complete(0);
}

int pread_cb(void *data, const void *buf, 
             size_t count, uint64_t offset,
             unsigned status, int *error) {
  *(unsigned *)data = status;
  return (0);
}

template <typename I>
void NBDStream<I>::read(io::Extents&& byte_extents, bufferlist* data,
                        Context* on_finish) {
  ldout(m_cct, 20) << "byte_extents=" << byte_extents << dendl;

  for (auto [byte_offset, byte_length] : byte_extents) {
    auto ptr = buffer::ptr_node::create(buffer::create_small_page_aligned(
      byte_length));
    auto buffer = boost::asio::mutable_buffer(ptr->c_str(), byte_length);
    data->push_back(std::move(ptr));
    unsigned read_status=0;
    if (nbd_pread_structured(nbd, boost::asio::buffer_cast<void *>(buffer),
      byte_length, byte_offset, 
      (nbd_chunk_callback) { .callback = pread_cb, .user_data = &read_status },
      0) == -1) {
      lderr(m_cct) << "failed to read" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    ldout(m_cct, 20) << "status=" << read_status << " offset=" << byte_offset 
      << " length=" << byte_length << dendl;
  }
  on_finish->complete(0);
}

int check_extent(void *data, 
                 const char *metacontext,
                 uint64_t offset,
                 uint32_t *entries, size_t nr_entries, int *error) {
  io::SparseExtents* sparse_extents = (io::SparseExtents*)data;
  uint64_t length = 0;
  for (size_t i=0; i<nr_entries; i+=2) {
    length += entries[i];
  }
  auto state = io::SPARSE_EXTENT_STATE_DATA;
  if (nr_entries == 2) {
    if (entries[1] & (LIBNBD_STATE_HOLE | LIBNBD_STATE_ZERO)) {
      state = io::SPARSE_EXTENT_STATE_ZEROED;
    }
  }
  sparse_extents->insert(offset, length, {state, length});
  
  return (0);
}

template <typename I>
void NBDStream<I>::list_snap(io::Extents&& image_extents,
                             io::SparseExtents* sparse_extents, 
                             Context* on_finish) {
  ldout(m_cct, 20) << "NBDStream::list_snap" << dendl;
  for (auto& [byte_offset, byte_length] : image_extents) {
    ldout(m_cct, 20) << "image_offset=" << byte_offset << dendl;
    if (nbd_block_status(nbd, byte_length, byte_offset,
      (nbd_extent_callback) { .callback = check_extent, .user_data = sparse_extents },
      0) == -1) {
      lderr(m_cct) << "nbd_block_status" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
  }

  on_finish->complete(0);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::NBDStream<librbd::ImageCtx>;
