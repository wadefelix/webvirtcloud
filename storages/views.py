import json
import os

from admin.decorators import superuser_only
from appsettings.settings import app_settings
from computes.models import Compute
from django.contrib import messages
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from libvirt import libvirtError
import paramiko

from vrtManager.connection import CONN_SSH, CONN_SOCKET
from vrtManager.storage import wvmStorage, wvmStorages

from storages.forms import AddStgPool, CloneImage, CreateVolumeForm


@superuser_only
def storages(request, compute_id):
    """
    :param request:
    :param compute_id:
    :return:
    """

    compute = get_object_or_404(Compute, pk=compute_id)
    errors = False

    try:
        conn = wvmStorages(
            compute.hostname, compute.login, compute.password, compute.type
        )
        storages = conn.get_storages_info()
        secrets = conn.get_secrets()

        if request.method == "POST":
            if "create" in request.POST:
                form = AddStgPool(request.POST)
                if form.is_valid():
                    data = form.cleaned_data
                    if data["name"] in storages:
                        msg = _("Pool name already use")
                        messages.error(request, msg)
                        errors = True
                    if data["stg_type"] == "rbd":
                        if not data["secret"]:
                            msg = _("You need create secret for pool")
                            messages.error(request, msg)
                            errors = True
                        if (
                            not data["ceph_pool"]
                            and not data["ceph_host"]
                            and not data["ceph_user"]
                        ):
                            msg = _("You need input all fields for creating ceph pool")
                            messages.error(request, msg)
                            errors = True
                    if not errors:
                        if data["stg_type"] == "rbd":
                            conn.create_storage_ceph(
                                data["stg_type"],
                                data["name"],
                                data["ceph_pool"],
                                data["ceph_host"],
                                data["ceph_user"],
                                data["secret"],
                            )
                        elif data["stg_type"] == "netfs":
                            conn.create_storage_netfs(
                                data["stg_type"],
                                data["name"],
                                data["netfs_host"],
                                data["source"],
                                data["source_format"],
                                data["target"],
                            )
                        else:
                            conn.create_storage(
                                data["stg_type"],
                                data["name"],
                                data["source"],
                                data["target"],
                            )
                        return HttpResponseRedirect(
                            reverse("storage", args=[compute_id, data["name"]])
                        )
                else:
                    for msg_err in form.errors.values():
                        messages.error(request, msg_err.as_text())
        conn.close()
    except libvirtError as lib_err:
        messages.error(request, lib_err)

    return render(request, "storages.html", locals())


@superuser_only
def storage(request, compute_id, pool):
    """
    :param request:
    :param compute_id:
    :param pool:
    :return:
    """
    def handle_uploaded_file(conn, path, file_name, file_chunk, is_last_chunk):
        temp_name = f"{file_name}.part"
        target_temp = os.path.normpath(os.path.join(path, temp_name))
        target_final = os.path.normpath(os.path.join(path, file_name))

        if not target_temp.startswith(path) or not target_final.startswith(path):
            raise Exception(_("Security Issues with file uploading"))

        if conn.conn == CONN_SSH:
            try:
                hostname, port = conn.host, 22
                if ":" in hostname:
                    hostname, port_str = hostname.split(":")
                    port = int(port_str)

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=hostname, port=port, username=conn.login, password=conn.passwd)
                sftp = ssh.open_sftp()

                remote_file = sftp.open(target_temp, 'ab')
                remote_file.set_pipelined(True)
                for chunk_data in file_chunk.chunks():
                    remote_file.write(chunk_data)
                remote_file.close()

                if is_last_chunk:
                    sftp.rename(target_temp, target_final)

                sftp.close()
                ssh.close()
            except Exception as e:
                raise Exception(_("SSH upload failed: {}").format(e))
        elif conn.conn == CONN_SOCKET:
            try:
                with open(target_temp, "ab") as f:
                    for chunk_data in file_chunk.chunks():
                        f.write(chunk_data)
                if is_last_chunk:
                    if os.path.exists(target_final):
                        os.remove(target_final)
                    os.rename(target_temp, target_final)
            except FileNotFoundError:
                raise Exception(_("File not found. Check the path variable and filename"))
        else:
            raise Exception(_("Unsupported connection type for file upload."))

    compute = get_object_or_404(Compute, pk=compute_id)
    meta_prealloc = False
    form = CreateVolumeForm()

    conn = wvmStorage(
        compute.hostname, compute.login, compute.password, compute.type, pool
    )

    storages = conn.get_storages()
    state = conn.is_active()
    try:
        size, free = conn.get_size()
        used = size - free
        if state:
            percent = (used * 100) // size
        else:
            percent = 0
    except libvirtError:
        size, free, used, percent = 0, 0, 0, 0

    status = conn.get_status()
    path = conn.get_target_path()
    type = conn.get_type()
    autostart = conn.get_autostart()

    if state:
        conn.refresh()
        volumes = conn.update_volumes()
    else:
        volumes = None

    if request.method == "POST":
        if "start" in request.POST:
            conn.start()
            return HttpResponseRedirect(request.get_full_path())
        if "stop" in request.POST:
            conn.stop()
            return HttpResponseRedirect(request.get_full_path())
        if "delete" in request.POST:
            conn.delete()
            return HttpResponseRedirect(reverse("storages", args=[compute_id]))
        if "set_autostart" in request.POST:
            conn.set_autostart(1)
            return HttpResponseRedirect(request.get_full_path())
        if "unset_autostart" in request.POST:
            conn.set_autostart(0)
            return HttpResponseRedirect(request.get_full_path())
        if "del_volume" in request.POST:
            volname = request.POST.get("volname", "")
            vol = conn.get_volume(volname)
            vol.delete(0)
            messages.success(
                request, _("Volume: %(vol)s is deleted.") % {"vol": volname}
            )
            return redirect(reverse("storage", args=[compute.id, pool]))
            # return HttpResponseRedirect(request.get_full_path())
        if "iso_upload" in request.POST:
            file_chunk = request.FILES.get("file")
            if not file_chunk:
                return JsonResponse({"error": _("No file chunk was submitted.")}, status=400)

            file_name = request.POST.get("file_name")
            chunk_index = int(request.POST.get("chunk_index", 0))
            total_chunks = int(request.POST.get("total_chunks", 1))
            is_last_chunk = chunk_index == total_chunks - 1

            # On first chunk, check if file already exists
            if chunk_index == 0:
                if file_name in conn.get_volumes():
                    return JsonResponse({"error": _("ISO image already exists")}, status=400)
                # Clean up any partial files from previous failed uploads
                temp_part_file = os.path.normpath(os.path.join(path, f"{file_name}.part"))
                if conn.conn == CONN_SOCKET and os.path.exists(temp_part_file):
                    os.remove(temp_part_file)
                elif conn.conn == CONN_SSH:
                    try:
                        hostname, port = conn.host, 22
                        if ":" in hostname:
                            hostname, port_str = hostname.split(":")
                            port = int(port_str)
                        ssh = paramiko.SSHClient()
                        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        ssh.connect(hostname=hostname, port=port, username=conn.login, password=conn.passwd)
                        sftp = ssh.open_sftp()
                        try:
                            sftp.remove(temp_part_file)
                        except FileNotFoundError:
                            pass # File doesn't exist, which is fine
                        sftp.close()
                        ssh.close()
                    except Exception:
                        # Best effort to clean up, if it fails, let it be.
                        pass

            try:
                handle_uploaded_file(conn, path, file_name, file_chunk, is_last_chunk)

                if is_last_chunk:
                    success_msg = _("ISO: %(file)s has been uploaded successfully.") % {"file": file_name}
                    messages.success(request, success_msg)
                    return JsonResponse({"success": True, "message": success_msg, "reload": True})
                else:
                    return JsonResponse({"success": True, "message": "Chunk received."})
            except Exception as e:
                error_msg = str(e)
                messages.error(request, error_msg)
                return JsonResponse({"error": error_msg}, status=500)
        if "cln_volume" in request.POST:
            form = CloneImage(request.POST)
            if form.is_valid():
                data = form.cleaned_data
                img_name = data["name"]
                meta_prealloc = 0
                if img_name in conn.update_volumes():
                    msg = _("Name of volume already in use")
                    messages.error(request, msg)
                if data["convert"]:
                    format = data["format"]
                    if data["meta_prealloc"] and data["format"] == "qcow2":
                        meta_prealloc = True
                else:
                    format = None
                try:
                    name = conn.clone_volume(
                        data["image"], data["name"], format, meta_prealloc
                    )
                    messages.success(
                        request,
                        _("%(image)s image cloned as %(name)s successfully")
                        % {"image": data["image"], "name": name},
                    )
                    return HttpResponseRedirect(request.get_full_path())
                except libvirtError as lib_err:
                    messages.error(request, lib_err)
            else:
                for msg_err in form.errors.values():
                    messages.error(request, msg_err.as_text())

    conn.close()

    return render(request, "storage.html", locals())


@superuser_only
def create_volume(request, compute_id, pool):
    """
    :param request:
    :param compute_id: compute id
    :param pool: pool name
    :return:
    """
    compute = get_object_or_404(Compute, pk=compute_id)
    meta_prealloc = False

    conn = wvmStorage(
        compute.hostname, compute.login, compute.password, compute.type, pool
    )

    storages = conn.get_storages()

    form = CreateVolumeForm(request.POST or None)
    if form.is_valid():
        data = form.cleaned_data
        if data["meta_prealloc"] and data["format"] == "qcow2":
            meta_prealloc = True

        disk_owner_uid = int(app_settings.INSTANCE_VOLUME_DEFAULT_OWNER_UID)
        disk_owner_gid = int(app_settings.INSTANCE_VOLUME_DEFAULT_OWNER_GID)

        name = conn.create_volume(
            data["name"],
            data["size"],
            data["format"],
            meta_prealloc,
            disk_owner_uid,
            disk_owner_gid,
        )
        messages.success(
            request, _("Image file %(name)s is created successfully") % {"name": name}
        )
    else:
        for msg_err in form.errors.values():
            messages.error(request, msg_err.as_text())

    return redirect(reverse("storage", args=[compute.id, pool]))


def get_volumes(request, compute_id, pool):
    """
    :param request:
    :param compute_id: compute id
    :param pool: pool name
    :return: volumes list of pool
    """
    data = {}
    compute = get_object_or_404(Compute, pk=compute_id)
    try:
        conn = wvmStorage(
            compute.hostname, compute.login, compute.password, compute.type, pool
        )
        conn.refresh()
        data["vols"] = sorted(conn.get_volumes())
    except libvirtError:
        pass
    return HttpResponse(json.dumps(data))
